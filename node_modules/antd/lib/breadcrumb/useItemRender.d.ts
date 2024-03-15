import * as React from 'react';
import type { BreadcrumbProps, ItemType } from './Breadcrumb';
type AddParameters<TFunction extends (...args: any) => any, TParameters extends [...args: any]> = (...args: [...Parameters<TFunction>, ...TParameters]) => ReturnType<TFunction>;
type ItemRender = NonNullable<BreadcrumbProps['itemRender']>;
type InternalItemRenderParams = AddParameters<ItemRender, [href?: string]>;
export declare function renderItem(prefixCls: string, item: ItemType, children: React.ReactNode, href?: string): React.JSX.Element | null;
export default function useItemRender(prefixCls: string, itemRender?: ItemRender): InternalItemRenderParams;
export {};
