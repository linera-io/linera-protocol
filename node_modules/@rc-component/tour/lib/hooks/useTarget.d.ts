import type { TourStepInfo } from '..';
export interface Gap {
    offset?: number | [number, number];
    radius?: number;
}
export interface PosInfo {
    left: number;
    top: number;
    height: number;
    width: number;
    radius: number;
}
export default function useTarget(target: TourStepInfo['target'], open: boolean, gap?: Gap, scrollIntoViewOptions?: boolean | ScrollIntoViewOptions): [PosInfo, HTMLElement];
