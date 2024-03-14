import type { BreadcrumbItemType, BreadcrumbSeparatorType, ItemType } from './Breadcrumb';
type MergedType = BreadcrumbItemType & {
    children?: ItemType['children'];
};
export default function useItems(items?: ItemType[], routes?: ItemType[]): Partial<MergedType & BreadcrumbSeparatorType>[] | null;
export {};
