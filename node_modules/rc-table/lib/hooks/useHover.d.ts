export type OnHover = (start: number, end: number) => void;
export default function useHover(): [startRow: number, endRow: number, onHover: OnHover];
