import * as React from 'react';
import type { KeyWiseTransferItem } from '.';
import type { RenderedItem, TransferListProps } from './list';
export declare const OmitProps: readonly ["handleFilter", "handleClear", "checkedKeys"];
export type OmitProp = typeof OmitProps[number];
type PartialTransferListProps<RecordType> = Omit<TransferListProps<RecordType>, OmitProp>;
export interface TransferListBodyProps<RecordType> extends PartialTransferListProps<RecordType> {
    filteredItems: RecordType[];
    filteredRenderItems: RenderedItem<RecordType>[];
    selectedKeys: string[];
}
export interface ListBodyRef<RecordType extends KeyWiseTransferItem> {
    items?: RenderedItem<RecordType>[];
}
declare const _default: React.ForwardRefExoticComponent<TransferListBodyProps<KeyWiseTransferItem> & React.RefAttributes<ListBodyRef<KeyWiseTransferItem>>>;
export default _default;
