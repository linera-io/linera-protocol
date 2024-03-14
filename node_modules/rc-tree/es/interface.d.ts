import * as React from 'react';
export type { ScrollTo } from 'rc-virtual-list/lib/List';
export interface TreeNodeProps<TreeDataType extends BasicDataNode = DataNode> {
    eventKey?: Key;
    prefixCls?: string;
    className?: string;
    style?: React.CSSProperties;
    id?: string;
    expanded?: boolean;
    selected?: boolean;
    checked?: boolean;
    loaded?: boolean;
    loading?: boolean;
    halfChecked?: boolean;
    title?: React.ReactNode | ((data: TreeDataType) => React.ReactNode);
    dragOver?: boolean;
    dragOverGapTop?: boolean;
    dragOverGapBottom?: boolean;
    pos?: string;
    domRef?: React.Ref<HTMLDivElement>;
    /** New added in Tree for easy data access */
    data?: TreeDataType;
    isStart?: boolean[];
    isEnd?: boolean[];
    active?: boolean;
    onMouseMove?: React.MouseEventHandler<HTMLDivElement>;
    isLeaf?: boolean;
    checkable?: boolean;
    selectable?: boolean;
    disabled?: boolean;
    disableCheckbox?: boolean;
    icon?: IconType;
    switcherIcon?: IconType;
    children?: React.ReactNode;
}
/** For fieldNames, we provides a abstract interface */
export interface BasicDataNode {
    checkable?: boolean;
    disabled?: boolean;
    disableCheckbox?: boolean;
    icon?: IconType;
    isLeaf?: boolean;
    selectable?: boolean;
    switcherIcon?: IconType;
    /** Set style of TreeNode. This is not recommend if you don't have any force requirement */
    className?: string;
    style?: React.CSSProperties;
}
/** Provide a wrap type define for developer to wrap with customize fieldNames data type */
export type FieldDataNode<T, ChildFieldName extends string = 'children'> = BasicDataNode & T & Partial<Record<ChildFieldName, FieldDataNode<T, ChildFieldName>[]>>;
export type Key = React.Key;
/**
 * Typescript not support `bigint` as index type yet.
 * We use this to mark the `bigint` type is for `Key` usage.
 * It's safe to remove this when typescript fix:
 * https://github.com/microsoft/TypeScript/issues/50217
 */
export type SafeKey = Exclude<Key, bigint>;
export type KeyEntities<DateType extends BasicDataNode = any> = Record<SafeKey, DataEntity<DateType>>;
export type DataNode = FieldDataNode<{
    key: Key;
    title?: React.ReactNode | ((data: DataNode) => React.ReactNode);
}>;
export type EventDataNode<TreeDataType> = {
    key: Key;
    expanded: boolean;
    selected: boolean;
    checked: boolean;
    loaded: boolean;
    loading: boolean;
    halfChecked: boolean;
    dragOver: boolean;
    dragOverGapTop: boolean;
    dragOverGapBottom: boolean;
    pos: string;
    active: boolean;
} & TreeDataType & BasicDataNode;
export type IconType = React.ReactNode | ((props: TreeNodeProps) => React.ReactNode);
export type NodeElement = React.ReactElement<TreeNodeProps> & {
    selectHandle?: HTMLSpanElement;
    type: {
        isTreeNode: boolean;
    };
};
export type NodeInstance<TreeDataType extends BasicDataNode = DataNode> = React.Component<TreeNodeProps<TreeDataType>> & {
    selectHandle?: HTMLSpanElement;
};
export interface Entity {
    node: NodeElement;
    index: number;
    key: Key;
    pos: string;
    parent?: Entity;
    children?: Entity[];
}
export interface DataEntity<TreeDataType extends BasicDataNode = DataNode> extends Omit<Entity, 'node' | 'parent' | 'children'> {
    node: TreeDataType;
    nodes: TreeDataType[];
    parent?: DataEntity<TreeDataType>;
    children?: DataEntity<TreeDataType>[];
    level: number;
}
export interface FlattenNode<TreeDataType extends BasicDataNode = DataNode> {
    parent: FlattenNode<TreeDataType> | null;
    children: FlattenNode<TreeDataType>[];
    pos: string;
    data: TreeDataType;
    title: React.ReactNode;
    key: Key;
    isStart: boolean[];
    isEnd: boolean[];
}
export type GetKey<RecordType> = (record: RecordType, index?: number) => Key;
export type GetCheckDisabled<RecordType> = (record: RecordType) => boolean;
export type Direction = 'ltr' | 'rtl' | undefined;
export interface FieldNames {
    title?: string;
    /** @private Internal usage for `rc-tree-select`, safe to remove if no need */
    _title?: string[];
    key?: string;
    children?: string;
}
