import * as React from 'react';
export type CompareProps<T extends React.ComponentType<any>> = (prevProps: Readonly<React.ComponentProps<T>>, nextProps: Readonly<React.ComponentProps<T>>) => boolean;
/**
 * Create Immutable pair for `makeImmutable` and `responseImmutable`.
 */
export default function createImmutable(): {
    makeImmutable: <T extends React.ComponentType<any>>(Component: T, shouldTriggerRender?: CompareProps<T>) => T;
    responseImmutable: <T_1 extends React.ComponentType<any>>(Component: T_1, propsAreEqual?: CompareProps<T_1>) => T_1;
    useImmutableMark: () => number;
};
