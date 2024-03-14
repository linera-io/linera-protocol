import * as React from 'react';
export interface MenuDividerProps extends React.HTMLAttributes<HTMLLIElement> {
    className?: string;
    prefixCls?: string;
    style?: React.CSSProperties;
    dashed?: boolean;
}
declare const MenuDivider: React.FC<MenuDividerProps>;
export default MenuDivider;
