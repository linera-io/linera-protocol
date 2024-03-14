import type { CSSMotionProps } from 'rc-motion';
export declare function getMotion(prefixCls: string, transitionName?: string): CSSMotionProps;
/** Wrap message open with promise like function */
export declare function wrapPromiseFn(openFn: (resolve: VoidFunction) => VoidFunction): any;
