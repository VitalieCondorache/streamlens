import { Routes } from '@angular/router';

/**
 * Every page is a lazy `loadComponent`: the live tail pulls in the virtual list and
 * the JSON inspector, the key router pulls in the hash playground — neither is part
 * of the initial bundle. Route `title` keeps the history and the a11y tree honest.
 */
export const routes: Routes = [
  { path: '', pathMatch: 'full', redirectTo: 'overview' },
  {
    path: 'overview',
    title: 'Overview · StreamLens',
    loadComponent: () => import('./features/overview/overview').then((m) => m.Overview),
  },
  {
    path: 'tail',
    title: 'Live tail · StreamLens',
    loadComponent: () => import('./features/live-tail/live-tail').then((m) => m.LiveTail),
  },
  {
    path: 'partitions',
    title: 'Partitions & consumer lag · StreamLens',
    loadComponent: () => import('./features/partitions/partitions').then((m) => m.Partitions),
  },
  {
    path: 'key-router',
    title: 'Key router · StreamLens',
    loadComponent: () => import('./features/key-router/key-router').then((m) => m.KeyRouter),
  },
  {
    path: 'architecture',
    title: 'How it works · StreamLens',
    loadComponent: () => import('./features/architecture/architecture').then((m) => m.Architecture),
  },
  { path: '**', redirectTo: 'overview' },
];
