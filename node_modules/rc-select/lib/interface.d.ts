import type * as React from 'react';
export type RawValueType = string | number;
export interface FlattenOptionData<OptionType> {
    label?: React.ReactNode;
    data: OptionType;
    key: React.Key;
    value?: RawValueType;
    groupOption?: boolean;
    group?: boolean;
}
export interface DisplayValueType {
    key?: React.Key;
    value?: RawValueType;
    label?: React.ReactNode;
    title?: React.ReactNode;
    disabled?: boolean;
}
export type RenderNode = React.ReactNode | ((props: any) => React.ReactNode);
export type RenderDOMFunc = (props: any) => HTMLElement;
export type Mode = 'multiple' | 'tags' | 'combobox';
export type Placement = 'bottomLeft' | 'bottomRight' | 'topLeft' | 'topRight';
export type DisplayInfoType = 'add' | 'remove' | 'clear';
