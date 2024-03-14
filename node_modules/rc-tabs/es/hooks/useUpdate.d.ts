/**
 * Help to merge callback with `useLayoutEffect`.
 * One time will only trigger once.
 */
export default function useUpdate(callback: VoidFunction): () => void;
type Callback<T> = (ori: T) => T;
export declare function useUpdateState<T>(defaultState: T | (() => T)): [T, (updater: Callback<T>) => void];
export {};
