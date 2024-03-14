import type { TriggerProps } from '..';
import type { AlignType } from '../interface';
export default function useAlign(open: boolean, popupEle: HTMLElement, target: HTMLElement | [x: number, y: number], placement: string, builtinPlacements: any, popupAlign?: AlignType, onPopupAlign?: TriggerProps['onPopupAlign']): [
    ready: boolean,
    offsetX: number,
    offsetY: number,
    offsetR: number,
    offsetB: number,
    arrowX: number,
    arrowY: number,
    scaleX: number,
    scaleY: number,
    align: AlignType,
    onAlign: VoidFunction
];
