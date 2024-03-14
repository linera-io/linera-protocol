import React from 'react';
export interface StarProps {
    value?: number;
    index?: number;
    prefixCls?: string;
    allowHalf?: boolean;
    disabled?: boolean;
    onHover?: (e: React.MouseEvent<HTMLDivElement>, index: number) => void;
    onClick?: (e: React.MouseEvent<HTMLDivElement> | React.KeyboardEvent<HTMLDivElement>, index: number) => void;
    character?: React.ReactNode | ((props: StarProps) => React.ReactNode);
    characterRender?: (origin: React.ReactElement, props: StarProps) => React.ReactNode;
    focused?: boolean;
    count?: number;
}
declare const _default: React.ForwardRefExoticComponent<StarProps & React.RefAttributes<HTMLLIElement>>;
export default _default;
