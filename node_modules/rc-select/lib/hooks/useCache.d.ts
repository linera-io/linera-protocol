import type { RawValueType } from '../BaseSelect';
import type { DefaultOptionType, LabelInValueType } from '../Select';
/**
 * Cache `value` related LabeledValue & options.
 */
declare const _default: (labeledValues: LabelInValueType[], valueOptions: Map<RawValueType, DefaultOptionType>) => [LabelInValueType[], (val: RawValueType) => DefaultOptionType];
export default _default;
