import type { DataEntity } from 'rc-tree/lib/interface';
import type { FieldNames, RawValueType } from '../TreeSelect';
declare const _default: (treeData: any, fieldNames: FieldNames) => {
    valueEntities: Map<RawValueType, DataEntity>;
    keyEntities: Record<string, DataEntity>;
};
export default _default;
