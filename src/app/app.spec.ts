import { provideHttpClient } from '@angular/common/http';
import { HttpTestingController, provideHttpClientTesting } from '@angular/common/http/testing';
import { TestBed } from '@angular/core/testing';
import { provideRouter } from '@angular/router';

import { App } from './app';
import { routes } from './app.routes';
import { DiagnosticsStore } from './core/diagnostics-store';

describe('App shell', () => {
  let http: HttpTestingController;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [App],
      providers: [provideRouter(routes), provideHttpClient(), provideHttpClientTesting()],
    }).compileComponents();

    http = TestBed.inject(HttpTestingController);
  });

  afterEach(() => {
    // The shell starts polling on construction; stop it so no timer outlives the test.
    TestBed.inject(DiagnosticsStore).stopPolling();
  });

  /**
   * The shell is expected to be useful with no backend at all, so these tests fail
   * every diagnostics call and still require a rendered, navigable page.
   * `whenStable` is avoided on purpose: with polling active it waits for requests
   * that the next tick keeps creating.
   */
  const renderWithoutBackend = async () => {
    const fixture = TestBed.createComponent(App);
    fixture.detectChanges();
    await Promise.resolve();

    for (const request of http.match(() => true)) {
      request.error(new ProgressEvent('error'));
    }

    // The fallback chain (probe -> recording -> offline) resolves over macrotasks.
    await new Promise((resolve) => setTimeout(resolve, 0));
    await new Promise((resolve) => setTimeout(resolve, 0));
    fixture.detectChanges();
    return fixture;
  };

  it('renders the shell with every section linked', async () => {
    const fixture = await renderWithoutBackend();
    const element = fixture.nativeElement as HTMLElement;
    const links = [...element.querySelectorAll('.nav a')];
    const labels = links.map((link) => link.textContent?.trim());

    expect(element.querySelector('.brand')?.textContent).toContain('StreamLens');
    expect(labels).toEqual([
      'Overview',
      'Live tail',
      'Partitions & lag',
      'Key router',
      'How it works',
    ]);

    // Regression guard: without `RouterLink` in the component imports these anchors
    // render href-less, so they neither navigate nor receive keyboard focus.
    expect(links.map((link) => link.getAttribute('href'))).toEqual([
      '/overview',
      '/tail',
      '/partitions',
      '/key-router',
      '/architecture',
    ]);
  });

  it('degrades gracefully when the BFF is unreachable', async () => {
    const fixture = await renderWithoutBackend();
    const element = fixture.nativeElement as HTMLElement;
    const status = element.querySelector('app-status-pill');

    expect(status).not.toBeNull();
    expect(status?.textContent).toMatch(/Recorded session|Offline/);
    expect(element.textContent).toContain('consumer group');
  });
});
