/// <reference types="react" />
export declare const STATUS_NONE: "none";
export declare const STATUS_APPEAR: "appear";
export declare const STATUS_ENTER: "enter";
export declare const STATUS_LEAVE: "leave";
export type MotionStatus = typeof STATUS_NONE | typeof STATUS_APPEAR | typeof STATUS_ENTER | typeof STATUS_LEAVE;
export declare const STEP_NONE: "none";
export declare const STEP_PREPARE: "prepare";
export declare const STEP_START: "start";
export declare const STEP_ACTIVE: "active";
export declare const STEP_ACTIVATED: "end";
/**
 * Used for disabled motion case.
 * Prepare stage will still work but start & active will be skipped.
 */
export declare const STEP_PREPARED: "prepared";
export type StepStatus = typeof STEP_NONE | typeof STEP_PREPARE | typeof STEP_START | typeof STEP_ACTIVE | typeof STEP_ACTIVATED | typeof STEP_PREPARED;
export type MotionEvent = (TransitionEvent | AnimationEvent) & {
    deadline?: boolean;
};
export type MotionPrepareEventHandler = (element: HTMLElement) => Promise<any> | void;
export type MotionEventHandler = (element: HTMLElement, event: MotionEvent) => React.CSSProperties | void;
export type MotionEndEventHandler = (element: HTMLElement, event: MotionEvent) => boolean | void;
