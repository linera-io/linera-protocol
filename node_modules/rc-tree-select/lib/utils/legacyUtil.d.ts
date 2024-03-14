import * as React from 'react';
import type { DataNode, ChangeEventExtra, RawValueType } from '../interface';
import type { DefaultOptionType, FieldNames } from '../TreeSelect';
export declare function convertChildrenToData(nodes: React.ReactNode): DataNode[];
export declare function fillLegacyProps(dataNode: DataNode): any;
export declare function fillAdditionalInfo(extra: ChangeEventExtra, triggerValue: RawValueType, checkedValues: RawValueType[], treeData: DefaultOptionType[], showPosition: boolean, fieldNames: FieldNames): void;
