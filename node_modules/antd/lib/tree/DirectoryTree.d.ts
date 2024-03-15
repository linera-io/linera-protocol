import * as React from 'react';
import type RcTree from 'rc-tree';
import type { BasicDataNode } from 'rc-tree';
import type { DataNode, Key } from 'rc-tree/lib/interface';
import type { TreeProps } from './Tree';
export type ExpandAction = false | 'click' | 'doubleClick';
export interface DirectoryTreeProps<T extends BasicDataNode = DataNode> extends TreeProps<T> {
    expandAction?: ExpandAction;
}
type DirectoryTreeCompoundedComponent = (<T extends BasicDataNode | DataNode = DataNode>(props: React.PropsWithChildren<DirectoryTreeProps<T>> & React.RefAttributes<RcTree>) => React.ReactElement) & Pick<React.FC, 'displayName'>;
export interface DirectoryTreeState {
    expandedKeys?: Key[];
    selectedKeys?: Key[];
}
declare const ForwardDirectoryTree: DirectoryTreeCompoundedComponent;
export default ForwardDirectoryTree;
