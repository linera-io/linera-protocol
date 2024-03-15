import type * as React from 'react';
export type SelectSource = 'option' | 'selection' | 'input' | 'clear';
export type Key = string | number;
export type RawValueType = string | number;
export interface LabelValueType {
    key?: Key;
    value?: RawValueType;
    label?: React.ReactNode;
    /** Only works on `treeCheckStrictly` */
    halfChecked?: boolean;
}
export type DefaultValueType = RawValueType | RawValueType[] | LabelValueType | LabelValueType[];
export interface DataNode {
    value?: RawValueType;
    title?: React.ReactNode;
    label?: React.ReactNode;
    key?: Key;
    disabled?: boolean;
    disableCheckbox?: boolean;
    checkable?: boolean;
    children?: DataNode[];
    /** Customize data info */
    [prop: string]: any;
}
export interface InternalDataEntity {
    key: Key;
    value: RawValueType;
    title?: React.ReactNode;
    disableCheckbox?: boolean;
    disabled?: boolean;
    children?: InternalDataEntity[];
    /** Origin DataNode */
    node: DataNode;
}
export interface LegacyDataNode extends DataNode {
    props: any;
}
export interface TreeDataNode extends DataNode {
    key: Key;
    children?: TreeDataNode[];
}
export interface FlattenDataNode {
    data: InternalDataEntity;
    key: Key;
    value: RawValueType;
    level: number;
    parent?: FlattenDataNode;
}
export interface SimpleModeConfig {
    id?: Key;
    pId?: Key;
    rootPId?: Key;
}
/** @deprecated This is only used for legacy compatible. Not works on new code. */
export interface LegacyCheckedNode {
    pos: string;
    node: React.ReactElement;
    children?: LegacyCheckedNode[];
}
export interface ChangeEventExtra {
    /** @deprecated Please save prev value by control logic instead */
    preValue: LabelValueType[];
    triggerValue: RawValueType;
    /** @deprecated Use `onSelect` or `onDeselect` instead. */
    selected?: boolean;
    /** @deprecated Use `onSelect` or `onDeselect` instead. */
    checked?: boolean;
    /** @deprecated This prop not work as react node anymore. */
    triggerNode: React.ReactElement;
    /** @deprecated This prop not work as react node anymore. */
    allCheckedNodes: LegacyCheckedNode[];
}
export interface FieldNames {
    value?: string;
    label?: string;
    children?: string;
}
