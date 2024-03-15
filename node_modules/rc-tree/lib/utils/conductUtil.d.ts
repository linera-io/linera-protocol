import { BasicDataNode, DataNode, GetCheckDisabled, Key, KeyEntities } from '../interface';
interface ConductReturnType {
    checkedKeys: Key[];
    halfCheckedKeys: Key[];
}
export declare function isCheckDisabled<TreeDataType>(node: TreeDataType): boolean;
/**
 * Conduct with keys.
 * @param keyList current key list
 * @param keyEntities key - dataEntity map
 * @param mode `fill` to fill missing key, `clean` to remove useless key
 */
export declare function conductCheck<TreeDataType extends BasicDataNode = DataNode>(keyList: Key[], checked: true | {
    checked: false;
    halfCheckedKeys: Key[];
}, keyEntities: KeyEntities<TreeDataType>, getCheckDisabled?: GetCheckDisabled<TreeDataType>): ConductReturnType;
export {};
