import type { CSSMotionProps } from 'rc-motion';
declare const initCollapseMotion: (rootCls?: string) => CSSMotionProps;
declare const SelectPlacements: readonly ["bottomLeft", "bottomRight", "topLeft", "topRight"];
export type SelectCommonPlacement = typeof SelectPlacements[number];
declare const getTransitionName: (rootPrefixCls: string, motion: string, transitionName?: string) => string;
export { getTransitionName };
export default initCollapseMotion;
