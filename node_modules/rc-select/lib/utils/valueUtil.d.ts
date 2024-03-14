import type { BaseOptionType, DefaultOptionType } from '../Select';
import type { FieldNames } from '../Select';
import type { FlattenOptionData } from '../interface';
export declare function isValidCount(value?: number): boolean;
export declare function fillFieldNames(fieldNames: FieldNames | undefined, childrenAsData: boolean): {
    label: string;
    value: string;
    options: string;
    groupLabel: string;
};
/**
 * Flat options into flatten list.
 * We use `optionOnly` here is aim to avoid user use nested option group.
 * Here is simply set `key` to the index if not provided.
 */
export declare function flattenOptions<OptionType extends BaseOptionType = DefaultOptionType>(options: OptionType[], { fieldNames, childrenAsData }?: {
    fieldNames?: FieldNames;
    childrenAsData?: boolean;
}): FlattenOptionData<OptionType>[];
/**
 * Inject `props` into `option` for legacy usage
 */
export declare function injectPropsWithOption<T extends object>(option: T): T;
export declare const getSeparatedContent: (text: string, tokens: string[], end?: number) => string[];
