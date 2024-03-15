/// <reference types="react" />
import type { ShowCheckedStrategy, SingleValueType, ValueType } from '../Cascader';
import type { GetEntities } from './useEntities';
export default function useSelect(multiple: boolean, triggerChange: (nextValues: ValueType) => void, checkedValues: SingleValueType[], halfCheckedValues: SingleValueType[], missingCheckedValues: SingleValueType[], getPathKeyEntities: GetEntities, getValueByKeyPath: (pathKeys: React.Key[]) => SingleValueType[], showCheckedStrategy: ShowCheckedStrategy): (valuePath: SingleValueType) => void;
