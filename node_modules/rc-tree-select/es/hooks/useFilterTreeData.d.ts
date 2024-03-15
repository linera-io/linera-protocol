import type { DefaultOptionType, InternalFieldName, TreeSelectProps } from '../TreeSelect';
declare const _default: (treeData: DefaultOptionType[], searchValue: string, { treeNodeFilterProp, filterTreeNode, fieldNames, }: {
    fieldNames: InternalFieldName;
    treeNodeFilterProp: string;
    filterTreeNode: TreeSelectProps['filterTreeNode'];
}) => DefaultOptionType[];
export default _default;
