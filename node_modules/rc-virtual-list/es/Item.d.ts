import * as React from 'react';
export interface ItemProps {
    children: React.ReactElement;
    setRef: (element: HTMLElement) => void;
}
export declare function Item({ children, setRef }: ItemProps): React.ReactElement<any, string | React.JSXElementConstructor<any>>;
