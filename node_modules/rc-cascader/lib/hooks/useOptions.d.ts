import * as React from 'react';
import type { DefaultOptionType } from '..';
import type { InternalFieldNames, SingleValueType } from '../Cascader';
import { type GetEntities } from './useEntities';
export default function useOptions(mergedFieldNames: InternalFieldNames, options?: DefaultOptionType[]): [
    mergedOptions: DefaultOptionType[],
    getPathKeyEntities: GetEntities,
    getValueByKeyPath: (pathKeys: React.Key[]) => SingleValueType[]
];
