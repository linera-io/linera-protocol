import type { ExpandableConfig, LegacyExpandableProps } from '../interface';
export declare const INTERNAL_COL_DEFINE = "RC_TABLE_INTERNAL_COL_DEFINE";
export declare function getExpandableProps<RecordType>(props: LegacyExpandableProps<RecordType> & {
    expandable?: ExpandableConfig<RecordType>;
}): ExpandableConfig<RecordType>;
