import { ChangeDetectionStrategy, Component } from '@angular/core';

/**
 * The engineering notes page. Static content on purpose: it is the part a reviewer
 * reads after the demo, and it belongs next to the code it explains.
 */
@Component({
  selector: 'app-architecture',
  changeDetection: ChangeDetectionStrategy.OnPush,
  templateUrl: './architecture.html',
  styleUrl: './architecture.scss',
})
export class Architecture {}
