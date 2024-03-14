import type { PlacementType } from './placements';
export declare function isInViewPort(element: HTMLElement): boolean;
export declare function getPlacement(targetElement?: HTMLElement | null, placement?: PlacementType, stepPlacement?: PlacementType): PlacementType;
