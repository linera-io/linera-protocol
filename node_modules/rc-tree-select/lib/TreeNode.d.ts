import type * as React from 'react';
import type { DataNode, Key } from './interface';
export interface TreeNodeProps extends Omit<DataNode, 'children'> {
    value: Key;
    children?: React.ReactNode;
}
/** This is a placeholder, not real render in dom */
declare const TreeNode: React.FC<TreeNodeProps>;
export default TreeNode;
