import * as React from 'react';
import type { MenuItemProps as RcMenuItemProps } from 'rc-menu';
export interface MenuItemProps extends Omit<RcMenuItemProps, 'title'> {
    icon?: React.ReactNode;
    danger?: boolean;
    title?: React.ReactNode;
}
type MenuItemComponent = React.FC<MenuItemProps>;
type RestArgs<T> = T extends (arg: any, ...args: infer P) => any ? P : never;
type GenericProps<T = unknown> = T extends infer U extends MenuItemProps ? unknown extends U ? MenuItemProps : U : MenuItemProps;
type GenericComponent = Omit<MenuItemComponent, ''> & {
    <T extends MenuItemProps>(props: GenericProps<T>, ...args: RestArgs<MenuItemComponent>): ReturnType<MenuItemComponent>;
};
declare const MenuItem: GenericComponent;
export default MenuItem;
