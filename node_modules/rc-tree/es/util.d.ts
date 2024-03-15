/**
 * Legacy code. Should avoid to use if you are new to import these code.
 */
import React from 'react';
import { BasicDataNode, DataEntity, DataNode, Direction, FlattenNode, Key, KeyEntities, NodeElement, NodeInstance } from './interface';
import { AllowDrop, TreeProps } from './Tree';
export { getPosition, isTreeNode } from './utils/treeUtil';
export declare function arrDel(list: Key[], value: Key): React.Key[];
export declare function arrAdd(list: Key[], value: Key): React.Key[];
export declare function posToArr(pos: string): string[];
export declare function getDragChildrenKeys<TreeDataType extends BasicDataNode = DataNode>(dragNodeKey: Key, keyEntities: KeyEntities<TreeDataType>): Key[];
export declare function isLastChild<TreeDataType extends BasicDataNode = DataNode>(treeNodeEntity: DataEntity<TreeDataType>): boolean;
export declare function isFirstChild<TreeDataType extends BasicDataNode = DataNode>(treeNodeEntity: DataEntity<TreeDataType>): boolean;
export declare function calcDropPosition<TreeDataType extends BasicDataNode = DataNode>(event: React.MouseEvent, dragNode: NodeInstance<TreeDataType>, targetNode: NodeInstance<TreeDataType>, indent: number, startMousePosition: {
    x: number;
    y: number;
}, allowDrop: AllowDrop<TreeDataType>, flattenedNodes: FlattenNode<TreeDataType>[], keyEntities: KeyEntities<TreeDataType>, expandKeys: Key[], direction: Direction): {
    dropPosition: -1 | 0 | 1;
    dropLevelOffset: number;
    dropTargetKey: Key;
    dropTargetPos: string;
    dropContainerKey: Key;
    dragOverNodeKey: Key;
    dropAllowed: boolean;
};
/**
 * Return selectedKeys according with multiple prop
 * @param selectedKeys
 * @param props
 * @returns [string]
 */
export declare function calcSelectedKeys(selectedKeys: Key[], props: TreeProps): React.Key[];
export declare function convertDataToTree(treeData: DataNode[], processor?: {
    processProps: (prop: DataNode) => any;
}): NodeElement[];
/**
 * Parse `checkedKeys` to { checkedKeys, halfCheckedKeys } style
 */
export declare function parseCheckedKeys(keys: Key[] | {
    checked: Key[];
    halfChecked: Key[];
}): any;
/**
 * If user use `autoExpandParent` we should get the list of parent node
 * @param keyList
 * @param keyEntities
 */
export declare function conductExpandParent(keyList: Key[], keyEntities: KeyEntities): Key[];
