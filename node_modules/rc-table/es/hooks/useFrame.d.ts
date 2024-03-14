export type Updater<State> = (prev: State) => State;
/**
 * Execute code before next frame but async
 */
export declare function useLayoutState<State>(defaultState: State): [State, (updater: Updater<State>) => void];
/** Lock frame, when frame pass reset the lock. */
export declare function useTimeoutLock<State>(defaultState?: State): [(state: State) => void, () => State | null];
