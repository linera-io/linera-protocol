/**
 * Callback will only execute last one for each raf
 */
export default function useRafDebounce(callback: VoidFunction): () => void;
