import * as React from 'react';
import type { AntTreeNodeProps, SwitcherIcon, TreeLeafIcon } from '../Tree';
interface SwitcherIconProps {
    prefixCls: string;
    treeNodeProps: AntTreeNodeProps;
    switcherIcon?: SwitcherIcon;
    showLine?: boolean | {
        showLeafIcon: boolean | TreeLeafIcon;
    };
}
declare const SwitcherIconCom: React.FC<SwitcherIconProps>;
export default SwitcherIconCom;
