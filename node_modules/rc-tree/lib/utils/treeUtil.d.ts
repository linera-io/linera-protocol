import * as React from 'react';
import { BasicDataNode, DataEntity, DataNode, EventDataNode, FieldNames, FlattenNode, GetKey, Key, KeyEntities, NodeElement, TreeNodeProps } from '../interface';
export declare function getPosition(level: string | number, index: number): string;
export declare function isTreeNode(node: NodeElement): boolean;
export declare function getKey(key: Key, pos: string): React.Key;
export declare function fillFieldNames(fieldNames?: FieldNames): Required<FieldNames>;
/**
 * Warning if TreeNode do not provides key
 */
export declare function warningWithoutKey(treeData: DataNode[], fieldNames: FieldNames): void;
/**
 * Convert `children` of Tree into `treeData` structure.
 */
export declare function convertTreeToData(rootNodes: React.ReactNode): DataNode[];
/**
 * Flat nest tree data into flatten list. This is used for virtual list render.
 * @param treeNodeList Origin data node list
 * @param expandedKeys
 * need expanded keys, provides `true` means all expanded (used in `rc-tree-select`).
 */
export declare function flattenTreeData<TreeDataType extends BasicDataNode = DataNode>(treeNodeList: TreeDataType[], expandedKeys: Key[] | true, fieldNames: FieldNames): FlattenNode<TreeDataType>[];
type ExternalGetKey = GetKey<DataNode> | string;
interface TraverseDataNodesConfig {
    childrenPropName?: string;
    externalGetKey?: ExternalGetKey;
    fieldNames?: FieldNames;
}
/**
 * Traverse all the data by `treeData`.
 * Please not use it out of the `rc-tree` since we may refactor this code.
 */
export declare function traverseDataNodes(dataNodes: DataNode[], callback: (data: {
    node: DataNode;
    index: number;
    pos: string;
    key: Key;
    parentPos: string | number;
    level: number;
    nodes: DataNode[];
}) => void, config?: TraverseDataNodesConfig | string): void;
interface Wrapper {
    posEntities: Record<string, DataEntity>;
    keyEntities: KeyEntities;
}
/**
 * Convert `treeData` into entity records.
 */
export declare function convertDataToEntities(dataNodes: DataNode[], { initWrapper, processEntity, onProcessFinished, externalGetKey, childrenPropName, fieldNames, }?: {
    initWrapper?: (wrapper: Wrapper) => Wrapper;
    processEntity?: (entity: DataEntity, wrapper: Wrapper) => void;
    onProcessFinished?: (wrapper: Wrapper) => void;
    externalGetKey?: ExternalGetKey;
    childrenPropName?: string;
    fieldNames?: FieldNames;
}, 
/** @deprecated Use `config.externalGetKey` instead */
legacyExternalGetKey?: ExternalGetKey): {
    posEntities: {};
    keyEntities: {};
};
export interface TreeNodeRequiredProps<TreeDataType extends BasicDataNode = DataNode> {
    expandedKeys: Key[];
    selectedKeys: Key[];
    loadedKeys: Key[];
    loadingKeys: Key[];
    checkedKeys: Key[];
    halfCheckedKeys: Key[];
    dragOverNodeKey: Key;
    dropPosition: number;
    keyEntities: KeyEntities<TreeDataType>;
}
/**
 * Get TreeNode props with Tree props.
 */
export declare function getTreeNodeProps<TreeDataType extends BasicDataNode = DataNode>(key: Key, { expandedKeys, selectedKeys, loadedKeys, loadingKeys, checkedKeys, halfCheckedKeys, dragOverNodeKey, dropPosition, keyEntities, }: TreeNodeRequiredProps<TreeDataType>): {
    eventKey: React.Key;
    expanded: boolean;
    selected: boolean;
    loaded: boolean;
    loading: boolean;
    checked: boolean;
    halfChecked: boolean;
    pos: string;
    dragOver: boolean;
    dragOverGapTop: boolean;
    dragOverGapBottom: boolean;
};
export declare function convertNodePropsToEventData<TreeDataType extends BasicDataNode = DataNode>(props: TreeNodeProps<TreeDataType>): EventDataNode<TreeDataType>;
export {};
