/**
 * Cache callback function that always return same ref instead.
 * This is used for context optimization.
 */
export default function useMemoCallback<T extends (...args: any[]) => void>(func: T): T;
