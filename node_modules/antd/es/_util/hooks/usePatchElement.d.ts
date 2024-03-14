import * as React from 'react';
export default function usePatchElement(): [
    React.ReactElement[],
    (element: React.ReactElement) => Function
];
