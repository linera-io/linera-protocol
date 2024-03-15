import * as React from 'react';
import { FlattenNode, TreeNodeProps } from './interface';
import { TreeNodeRequiredProps } from './utils/treeUtil';
interface MotionTreeNodeProps extends Omit<TreeNodeProps, 'domRef'> {
    active: boolean;
    motion?: any;
    motionNodes?: FlattenNode[];
    onMotionStart: () => void;
    onMotionEnd: () => void;
    motionType?: 'show' | 'hide';
    treeNodeRequiredProps: TreeNodeRequiredProps;
}
declare const RefMotionTreeNode: React.ForwardRefExoticComponent<MotionTreeNodeProps & React.RefAttributes<HTMLDivElement>>;
export default RefMotionTreeNode;
