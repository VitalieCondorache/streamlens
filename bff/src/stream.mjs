/**
 * Server-Sent Events hub — the bridge between a Kafka consumer group and a browser.
 *
 * Design decisions worth calling out:
 *
 * 1. The consumer group id is derived from a *browser session id* kept in
 *    localStorage. Committed offsets are therefore the resume point: a refresh
 *    continues exactly where the previous connection stopped (Kafka's own
 *    at-least-once semantics, no custom bookkeeping).
 * 2. Events are batched per flush window and written with a single `res.write()`.
 *    When that write returns `false` the socket is saturated, so we pause the
 *    consumer until `drain` — real backpressure instead of unbounded buffering.
 * 3. Every frame carries `id: topic:partition:offset`. Delivered at-least-once,
 *    deduplicated by the client through per-partition watermarks.
 */
import { randomUUID } from 'node:crypto';
import { config, TOPICS, topicNames } from './config.mjs';
import { log } from './log.mjs';

const SSE_HEADERS = {
  'Content-Type': 'text/event-stream; charset=utf-8',
  'Cache-Control': 'no-cache, no-transform',
  Connection: 'keep-alive',
  'X-Accel-Buffering': 'no',
};

const frame = ({ id, event, data }) => {
  const lines = [];
  if (id) lines.push(`id: ${id}`);
  if (event) lines.push(`event: ${event}`);
  lines.push(`data: ${typeof data === 'string' ? data : JSON.stringify(data)}`);
  return `${lines.join('\n')}\n\n`;
};

const eventId = (topic, partition, offset) => `${topic}:${partition}:${offset}`;

const topicPartitions = (topics) =>
  TOPICS.filter((topic) => topics.includes(topic.name)).map((topic) => ({
    topic: topic.name,
    partitions: Array.from({ length: topic.partitions }, (_, index) => index),
  }));

export const createStreamHub = (bridge, logger = log) => {
  const connections = new Map();
  const stats = {
    opened: 0,
    closed: 0,
    frames: 0,
    events: 0,
    backpressurePauses: 0,
    supersededConnections: 0,
    crashedConsumers: 0,
  };

  const write = (connection, chunk) => {
    try {
      return connection.res.write(chunk);
    } catch (error) {
      logger.warn('sse write failed', { error: error.message });
      return false;
    }
  };

  const flush = (connection) => {
    if (connection.buffer.length === 0) return;
    const chunk = connection.buffer.join('');
    connection.buffer.length = 0;
    stats.frames += 1;

    if (!write(connection, chunk) && !connection.paused) {
      connection.paused = true;
      stats.backpressurePauses += 1;
      connection.consumer.pause(topicPartitions(connection.topics));
      connection.buffer.push(
        frame({
          event: 'stream-notice',
          data: {
            level: 'warn',
            code: 'backpressure',
            message: 'Client is slow: consumer paused until the socket drains.',
          },
        }),
      );
      logger.warn('sse backpressure: consumer paused', { session: connection.sessionId });

      connection.res.once('drain', () => {
        connection.paused = false;
        connection.consumer.resume(topicPartitions(connection.topics));
        connection.buffer.push(
          frame({
            event: 'stream-notice',
            data: { level: 'info', code: 'resumed', message: 'Socket drained: consumer resumed.' },
          }),
        );
        logger.info('sse backpressure released', { session: connection.sessionId });
      });
    }
  };

  const close = async (sessionId, reason) => {
    const connection = connections.get(sessionId);
    if (!connection) return;
    connections.delete(sessionId);
    clearInterval(connection.flushTimer);
    clearInterval(connection.heartbeatTimer);
    stats.closed += 1;
    try {
      await connection.consumer.disconnect();
    } catch (error) {
      logger.debug('consumer disconnect failed', { error: error.message });
    }
    connection.res.end();
    logger.info('sse session closed', { session: sessionId, reason, events: connection.delivered });
  };
  const handle = async (req, res, url) => {
    const rawSession = url.searchParams.get('session') ?? '';
    const sessionId = rawSession.replace(/[^a-zA-Z0-9_-]/g, '').slice(0, 48) || randomUUID();
    const requested = (url.searchParams.get('topics') ?? '')
      .split(',')
      .map((name) => name.trim())
      .filter((name) => topicNames.includes(name));
    const topics = requested.length > 0 ? requested : topicNames;
    const fromBeginning = url.searchParams.get('from') === 'earliest';
    const groupId = `streamlens-ui-${sessionId}`;

    // A refresh leaves a half-dead connection behind: drop the old one first so
    // we never leak consumer group members.
    if (connections.has(sessionId)) {
      stats.supersededConnections += 1;
      await close(sessionId, 'superseded by a newer connection');
    }

    res.writeHead(200, {
      ...SSE_HEADERS,
      'Access-Control-Allow-Origin': config.corsOrigin,
      'X-StreamLens-Session': sessionId,
    });
    res.write(`retry: 2000\n\n`);
    res.write(
      frame({
        event: 'stream-ready',
        data: {
          sessionId,
          groupId,
          topics,
          fromBeginning,
          flushMs: config.stream.flushMs,
          kafka: config.brokers,
        },
      }),
    );

    const connection = {
      sessionId,
      groupId,
      topics,
      res,
      buffer: [],
      paused: false,
      delivered: 0,
      openedAt: Date.now(),
      flushTimer: null,
      heartbeatTimer: null,
      consumer: null,
    };
    connections.set(sessionId, connection);
    stats.opened += 1;

    connection.flushTimer = setInterval(() => flush(connection), config.stream.flushMs);
    connection.heartbeatTimer = setInterval(() => {
      if (connection.buffer.length > config.stream.maxBuffer) {
        // The client never drained. Protecting the process beats buffering forever.
        logger.warn('sse buffer limit reached, dropping session', {
          session: sessionId,
          buffered: connection.buffer.length,
        });
        void close(sessionId, 'client buffer overflow');
        return;
      }
      if (connection.buffer.length === 0) write(connection, `: heartbeat ${Date.now()}\n\n`);
    }, config.stream.heartbeatMs);

    req.on('close', () => void close(sessionId, 'client disconnected'));

    const consumer = bridge.newConsumer({ groupId });
    connection.consumer = consumer;

    consumer.on(consumer.events.GROUP_JOIN, ({ payload }) => {
      connection.buffer.push(
        frame({
          event: 'stream-notice',
          data: {
            level: 'info',
            code: 'group-join',
            message: `Consumer joined ${payload.groupId}`,
            assignment: payload.memberAssignment ?? {},
          },
        }),
      );
    });
    consumer.on(consumer.events.REBALANCING, () => {
      connection.buffer.push(
        frame({
          event: 'stream-notice',
          data: { level: 'warn', code: 'rebalancing', message: 'Group is rebalancing.' },
        }),
      );
    });
    consumer.on(consumer.events.CRASH, ({ payload }) => {
      stats.crashedConsumers += 1;
      logger.error('consumer crashed', { session: sessionId, error: payload.error?.message });
      connection.buffer.push(
        frame({
          event: 'stream-notice',
          data: {
            level: 'error',
            code: 'consumer-crash',
            message: payload.error?.message ?? 'consumer crashed',
          },
        }),
      );
    });

    try {
      await consumer.connect();
      await consumer.subscribe({ topics, fromBeginning });
      await consumer.run({
        autoCommit: true,
        autoCommitInterval: 1_000,
        eachBatch: async ({ batch }) => {
          for (const message of batch.messages) {
            let decoded;
            try {
              decoded = JSON.parse(message.value?.toString() ?? 'null');
            } catch {
              decoded = { type: 'unparseable', payload: {} };
            }

            connection.delivered += 1;
            stats.events += 1;
            connection.buffer.push(
              frame({
                id: eventId(batch.topic, batch.partition, message.offset),
                event: 'stream-event',
                data: {
                  id: eventId(batch.topic, batch.partition, message.offset),
                  topic: batch.topic,
                  partition: batch.partition,
                  offset: message.offset,
                  key: message.key ? message.key.toString() : null,
                  type: decoded?.type ?? 'unknown',
                  at: Number(decoded?.at ?? message.timestamp),
                  payload: decoded?.payload ?? {},
                  ingestedAt: Date.now(),
                },
              }),
            );
          }
        },
      });
    } catch (error) {
      logger.error('sse consumer failed to start', { session: sessionId, error: error.message });
      write(
        connection,
        frame({
          event: 'stream-notice',
          data: { level: 'error', code: 'consumer-failed', message: error.message },
        }),
      );
      await close(sessionId, `consumer failed: ${error.message}`);
    }
  };

  return {
    handle,
    close,
    isActive: (sessionId) => connections.has(sessionId),
    closeAll: () =>
      Promise.all([...connections.keys()].map((id) => close(id, 'server shutting down'))),
    stats: () => ({
      ...stats,
      activeSessions: connections.size,
      sessions: [...connections.values()].map((connection) => ({
        sessionId: connection.sessionId,
        groupId: connection.groupId,
        topics: connection.topics,
        delivered: connection.delivered,
        buffered: connection.buffer.length,
        paused: connection.paused,
        openedAt: connection.openedAt,
      })),
    }),
  };
};
