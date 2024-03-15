import * as React from 'react';
export default function useMobileTouchMove(inVirtual: boolean, listRef: React.RefObject<HTMLDivElement>, callback: (offsetY: number, smoothOffset?: boolean) => boolean): void;
