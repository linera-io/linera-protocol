import * as React from 'react';
import type { MenuItemType } from './interface';
export interface MenuItemProps extends Omit<MenuItemType, 'label' | 'key'>, Omit<React.HTMLAttributes<HTMLLIElement>, 'onClick' | 'onMouseEnter' | 'onMouseLeave' | 'onSelect'> {
    children?: React.ReactNode;
    /** @private Internal filled key. Do not set it directly */
    eventKey?: string;
    /** @private Do not use. Private warning empty usage */
    warnKey?: boolean;
    /** @deprecated No place to use this. Should remove */
    attribute?: Record<string, string>;
}
declare const _default: React.ForwardRefExoticComponent<MenuItemProps & React.RefAttributes<HTMLElement>>;
export default _default;
