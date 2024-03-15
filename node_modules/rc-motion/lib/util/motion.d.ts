import { MotionName } from '../CSSMotion';
export declare function getVendorPrefixes(domSupport: boolean, win: object): {
    animationend: Record<string, string>;
    transitionend: Record<string, string>;
};
export declare function getVendorPrefixedEventName(eventName: string): any;
export declare const supportTransition: boolean;
export declare const animationEndName: any;
export declare const transitionEndName: any;
export declare function getTransitionName(transitionName: MotionName, transitionType: string): any;
