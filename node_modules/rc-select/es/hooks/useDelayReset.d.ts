/**
 * Similar with `useLock`, but this hook will always execute last value.
 * When set to `true`, it will keep `true` for a short time even if `false` is set.
 */
export default function useDelayReset(timeout?: number): [boolean, (val: boolean, callback?: () => void) => void, () => void];
