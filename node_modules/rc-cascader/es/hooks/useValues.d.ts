import type { DataEntity } from 'rc-tree/lib/interface';
import * as React from 'react';
import type { SingleValueType } from '../Cascader';
import type { GetMissValues } from './useMissingValues';
export default function useValues(multiple: boolean, rawValues: SingleValueType[], getPathKeyEntities: () => Record<string, DataEntity>, getValueByKeyPath: (pathKeys: React.Key[]) => SingleValueType[], getMissingValues: GetMissValues): [
    checkedValues: SingleValueType[],
    halfCheckedValues: SingleValueType[],
    missingCheckedValues: SingleValueType[]
];
