export type Trigger = (callback?: VoidFunction) => void;
/**
 * Trigger a callback on state change
 */
export default function useEffectState(): Trigger;
