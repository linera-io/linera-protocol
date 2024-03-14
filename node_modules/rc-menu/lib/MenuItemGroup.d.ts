import * as React from 'react';
import type { MenuItemGroupType } from './interface';
export interface MenuItemGroupProps extends Omit<MenuItemGroupType, 'type' | 'children' | 'label'> {
    title?: React.ReactNode;
    children?: React.ReactNode;
    /** @private Internal filled key. Do not set it directly */
    eventKey?: string;
    /** @private Do not use. Private warning empty usage */
    warnKey?: boolean;
}
export default function MenuItemGroup({ children, ...props }: MenuItemGroupProps): React.ReactElement;
