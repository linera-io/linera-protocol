import * as React from 'react';
export interface ItemProps {
    className: string;
    children: React.ReactNode;
    index: number;
    split?: React.ReactNode;
    style?: React.CSSProperties;
}
declare const Item: React.FC<ItemProps>;
export default Item;
