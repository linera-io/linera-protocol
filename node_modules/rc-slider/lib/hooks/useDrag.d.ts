import * as React from 'react';
import type { Direction, OnStartMove } from '../interface';
import type { OffsetValues } from './useOffset';
export default function useDrag(containerRef: React.RefObject<HTMLDivElement>, direction: Direction, rawValues: number[], min: number, max: number, formatValue: (value: number) => number, triggerChange: (values: number[]) => void, finishChange: () => void, offsetValues: OffsetValues): [number, number, number[], OnStartMove];
