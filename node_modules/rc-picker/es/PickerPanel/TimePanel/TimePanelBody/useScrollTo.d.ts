import * as React from 'react';
export default function useScrollTo(ulRef: React.RefObject<HTMLUListElement>, value: number | string): [syncScroll: VoidFunction, clearScroll: VoidFunction, isScrolling: () => boolean];
