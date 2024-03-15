/**
 * Trigger `callback` immediately when `condition` is `true`.
 * But trigger `callback` in next frame when `condition` is `false`.
 */
export default function useLockEffect(condition: boolean, callback: (next: boolean) => void, delayFrames?: number): void;
