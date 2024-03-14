import type { CSSMotionProps } from 'rc-motion';
import type { AlignType, AnimationType, BuildInPlacements, TransitionNameType } from './interface';
export declare function getAlignPopupClassName(builtinPlacements: BuildInPlacements, prefixCls: string, align: AlignType, isAlignPoint: boolean): string;
/** @deprecated We should not use this if we can refactor all deps */
export declare function getMotion(prefixCls: string, motion: CSSMotionProps, animation: AnimationType, transitionName: TransitionNameType): CSSMotionProps;
export declare function getWin(ele: HTMLElement): Window & typeof globalThis;
/**
 * Get all the scrollable parent elements of the element
 * @param ele       The element to be detected
 * @param areaOnly  Only return the parent which will cut visible area
 */
export declare function collectScroller(ele: HTMLElement): HTMLElement[];
export declare function toNum(num: number, defaultValue?: number): number;
export interface VisibleArea {
    left: number;
    top: number;
    right: number;
    bottom: number;
}
/**
 *
 *
 *  **************************************
 *  *              Border                *
 *  *     **************************     *
 *  *     *                  *     *     *
 *  *  B  *                  *  S  *  B  *
 *  *  o  *                  *  c  *  o  *
 *  *  r  *      Content     *  r  *  r  *
 *  *  d  *                  *  o  *  d  *
 *  *  e  *                  *  l  *  e  *
 *  *  r  ********************  l  *  r  *
 *  *     *        Scroll          *     *
 *  *     **************************     *
 *  *              Border                *
 *  **************************************
 *
 */
/**
 * Get visible area of element
 */
export declare function getVisibleArea(initArea: VisibleArea, scrollerList?: HTMLElement[]): {
    left: number;
    top: number;
    right: number;
    bottom: number;
};
