/**
 * Same as `React.useCallback` but always return a memoized function
 * but redirect to real function.
 */
export default function useRefFunc<T extends (...args: any[]) => any>(callback: T): T;
