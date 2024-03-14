import type * as React from 'react';
import type { DataNode, FieldNames } from '../interface';
import type { DefaultOptionType, InternalFieldName } from '../TreeSelect';
export declare function toArray<T>(value: T | T[]): T[];
export declare function fillFieldNames(fieldNames?: FieldNames): {
    _title: string[];
    value: string;
    key: string;
    children: string;
};
export declare function isCheckDisabled(node: DataNode): boolean;
/** Loop fetch all the keys exist in the tree */
export declare function getAllKeys(treeData: DefaultOptionType[], fieldNames: InternalFieldName): React.Key[];
export declare function isNil(val: any): boolean;
