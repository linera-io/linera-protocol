import type { ExpandableConfig, ExpandableType, GetRowKey, Key, RenderExpandIcon, TriggerEventHandler } from '../interface';
import type { TableProps } from '../Table';
export default function useExpand<RecordType>(props: TableProps<RecordType>, mergedData: readonly RecordType[], getRowKey: GetRowKey<RecordType>): [
    expandableConfig: ExpandableConfig<RecordType>,
    expandableType: ExpandableType,
    expandedKeys: Set<Key>,
    expandIcon: RenderExpandIcon<unknown>,
    childrenColumnName: string,
    onTriggerExpand: TriggerEventHandler<RecordType>
];
