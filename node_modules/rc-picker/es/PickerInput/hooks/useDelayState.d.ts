/**
 * Will be `true` immediately for next effect.
 * But will be `false` for a delay of effect.
 */
export default function useDelayState<T>(value: T, defaultValue?: T, onChange?: (next: T) => void): [state: T, setState: (nextState: T, immediately?: boolean) => void];
