export default function useRefs<T>(): [
    getRef: (index: number) => T,
    setRef: (index: number) => (instance: T) => void
];
