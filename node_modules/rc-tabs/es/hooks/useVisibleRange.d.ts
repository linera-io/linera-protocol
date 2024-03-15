import type { Tab, TabOffsetMap } from '../interface';
import type { TabNavListProps } from '../TabNavList';
export type ContainerSizeInfo = [width: number, height: number, left: number, top: number];
export default function useVisibleRange(tabOffsets: TabOffsetMap, visibleTabContentValue: number, transform: number, tabContentSizeValue: number, addNodeSizeValue: number, operationNodeSizeValue: number, { tabs, tabPosition, rtl }: {
    tabs: Tab[];
} & TabNavListProps): [visibleStart: number, visibleEnd: number];
