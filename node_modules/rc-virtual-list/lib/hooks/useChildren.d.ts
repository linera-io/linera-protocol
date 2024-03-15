import * as React from 'react';
import type { SharedConfig, RenderFunc } from '../interface';
export default function useChildren<T>(list: T[], startIndex: number, endIndex: number, scrollWidth: number, setNodeRef: (item: T, element: HTMLElement) => void, renderFunc: RenderFunc<T>, { getKey }: SharedConfig<T>): React.JSX.Element[];
