import type { BuildInPlacements } from '@rc-component/trigger';
import { getArrowOffsetToken } from '../style/placementArrow';
export interface AdjustOverflow {
    adjustX?: 0 | 1;
    adjustY?: 0 | 1;
}
export interface PlacementsConfig {
    arrowWidth: number;
    arrowPointAtCenter?: boolean;
    autoAdjustOverflow?: boolean | AdjustOverflow;
    offset: number;
    borderRadius: number;
    visibleFirst?: boolean;
}
export declare function getOverflowOptions(placement: string, arrowOffset: ReturnType<typeof getArrowOffsetToken>, arrowWidth: number, autoAdjustOverflow?: boolean | AdjustOverflow): {
    adjustX?: number | boolean | undefined;
    adjustY?: number | boolean | undefined;
    shiftX?: number | boolean | undefined;
    shiftY?: number | boolean | undefined;
};
export default function getPlacements(config: PlacementsConfig): BuildInPlacements;
