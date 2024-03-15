import * as React from 'react';
import { TreeContextProps } from './contextTypes';
import { TreeNodeProps } from './interface';
export type { TreeNodeProps } from './interface';
export interface InternalTreeNodeProps extends TreeNodeProps {
    context?: TreeContextProps;
}
export interface TreeNodeState {
    dragNodeHighlight: boolean;
}
declare const ContextTreeNode: React.FC<TreeNodeProps>;
export default ContextTreeNode;
