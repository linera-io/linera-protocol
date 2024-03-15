import type { BuildInPlacements } from '@rc-component/trigger';
export type PlacementType = 'left' | 'leftTop' | 'leftBottom' | 'right' | 'rightTop' | 'rightBottom' | 'top' | 'topLeft' | 'topRight' | 'bottom' | 'bottomLeft' | 'bottomRight' | 'center';
export declare function getPlacements(arrowPointAtCenter?: boolean): BuildInPlacements;
export declare const placements: BuildInPlacements;
