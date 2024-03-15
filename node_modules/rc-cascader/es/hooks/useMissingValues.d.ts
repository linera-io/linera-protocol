import type { DefaultOptionType, InternalFieldNames, SingleValueType } from '../Cascader';
export type GetMissValues = ReturnType<typeof useMissingValues>;
export default function useMissingValues(options: DefaultOptionType[], fieldNames: InternalFieldNames): (rawValues: SingleValueType[]) => [SingleValueType[], SingleValueType[]];
