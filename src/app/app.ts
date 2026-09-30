import { Component, inject, signal } from '@angular/core';
import { RouterLink, RouterLinkActive, RouterOutlet } from '@angular/router';

import { DiagnosticsStore } from './core/diagnostics-store';
import { StreamService } from './core/stream-service';
import { StreamStore } from './core/stream/stream-store';
import { ThemeStore } from './core/theme-store';
import { StatusPill } from './shared/ui/status-pill/status-pill';

@Component({
  selector: 'app-root',
  imports: [RouterLink, RouterLinkActive, RouterOutlet, StatusPill],
  templateUrl: './app.html',
  styleUrl: './app.scss',
})
export class App {
  private readonly stream = inject(StreamService);
  private readonly diagnostics = inject(DiagnosticsStore);

  protected readonly store = inject(StreamStore);
  protected readonly theme = inject(ThemeStore);
  protected readonly menuOpen = signal(false);

  constructor() {
    // One call decides live vs recorded replay; from here on the state is reactive.
    void this.stream.start();
    this.diagnostics.startPolling();
  }

  protected reconnect(): void {
    void this.stream.reconnect();
  }
}
