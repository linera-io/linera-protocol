import * as React from 'react';
import type { FieldNames, RawValueType } from '../Select';
/**
 * Parse `children` to `options` if `options` is not provided.
 * Then flatten the `options`.
 */
declare const useOptions: <OptionType>(options: OptionType[], children: React.ReactNode, fieldNames: FieldNames, optionFilterProp: string, optionLabelProp: string) => {
    options: OptionType[];
    valueOptions: Map<RawValueType, OptionType>;
    labelOptions: Map<React.ReactNode, OptionType>;
};
export default useOptions;
