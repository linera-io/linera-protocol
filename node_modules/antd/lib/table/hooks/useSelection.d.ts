import * as React from 'react';
import type { AnyObject } from '../../_util/type';
import type { ExpandType, GetPopupContainer, GetRowKey, Key, SelectionItem, TableLocale, TableRowSelection, TransformColumns } from '../interface';
export declare const SELECTION_COLUMN: {};
export declare const SELECTION_ALL: "SELECT_ALL";
export declare const SELECTION_INVERT: "SELECT_INVERT";
export declare const SELECTION_NONE: "SELECT_NONE";
interface UseSelectionConfig<RecordType extends AnyObject = AnyObject> {
    prefixCls: string;
    pageData: RecordType[];
    data: RecordType[];
    getRowKey: GetRowKey<RecordType>;
    getRecordByKey: (key: Key) => RecordType;
    expandType: ExpandType;
    childrenColumnName: string;
    locale: TableLocale;
    getPopupContainer?: GetPopupContainer;
}
export type INTERNAL_SELECTION_ITEM = SelectionItem | typeof SELECTION_ALL | typeof SELECTION_INVERT | typeof SELECTION_NONE;
declare const useSelection: <RecordType extends AnyObject = AnyObject>(config: UseSelectionConfig<RecordType>, rowSelection?: TableRowSelection<RecordType> | undefined) => readonly [TransformColumns<RecordType>, Set<React.Key>];
export default useSelection;
