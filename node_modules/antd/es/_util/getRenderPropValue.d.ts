import type * as React from 'react';
export type RenderFunction = () => React.ReactNode;
export declare const getRenderPropValue: (propValue?: React.ReactNode | RenderFunction) => React.ReactNode;
