import type { OnHover } from '../hooks/useHover';
export default function useHoverState(rowIndex: number, rowSpan: number): [hovering: boolean, onHover: OnHover];
