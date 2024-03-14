/// <reference types="react" />
import type { SingleValueType, DefaultOptionType, InternalFieldNames, ShowCheckedStrategy } from '../Cascader';
import type { GetEntities } from '../hooks/useEntities';
export declare function formatStrategyValues(pathKeys: React.Key[], getKeyPathEntities: GetEntities, showCheckedStrategy: ShowCheckedStrategy): import("react").Key[];
export declare function toPathOptions(valueCells: SingleValueType, options: DefaultOptionType[], fieldNames: InternalFieldNames, stringMode?: boolean): {
    value: SingleValueType[number];
    index: number;
    option: DefaultOptionType;
}[];
