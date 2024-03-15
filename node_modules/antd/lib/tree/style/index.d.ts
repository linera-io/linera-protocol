/// <reference types="react" />
import type { CSSInterpolation, CSSObject } from '@ant-design/cssinjs';
import type { AliasToken, FullToken, GetDefaultToken } from '../../theme/internal';
import type { CSSUtil } from '../../theme/util/genComponentStyleHook';
export interface TreeSharedToken {
    /**
     * @desc 节点标题高度
     * @descEN Node title height
     */
    titleHeight: number;
    /**
     * @desc 节点悬浮态背景色
     * @descEN Background color of hovered node
     */
    nodeHoverBg: string;
    /**
     * @desc 节点选中态背景色
     * @descEN Background color of selected node
     */
    nodeSelectedBg: string;
}
export interface ComponentToken extends TreeSharedToken {
    /**
     * @desc 目录树节点选中文字颜色
     * @descEN Text color of selected directory node
     */
    directoryNodeSelectedColor: string;
    /**
     * @desc 目录树节点选中背景色
     * @descEN Background color of selected directory node
     */
    directoryNodeSelectedBg: string;
}
type TreeToken = FullToken<'Tree'> & {
    treeCls: string;
    treeNodeCls: string;
    treeNodePadding: number | string;
};
export declare const genBaseStyle: (prefixCls: string, token: TreeToken) => CSSObject;
export declare const genDirectoryStyle: (token: TreeToken) => CSSObject;
export declare const genTreeStyle: (prefixCls: string, token: AliasToken & TreeSharedToken & CSSUtil) => CSSInterpolation;
export declare const initComponentToken: (token: AliasToken) => TreeSharedToken;
export declare const prepareComponentToken: GetDefaultToken<'Tree'>;
declare const _default: (prefixCls: string, rootCls?: string) => readonly [(node: import("react").ReactElement<any, string | import("react").JSXElementConstructor<any>>) => import("react").ReactElement<any, string | import("react").JSXElementConstructor<any>>, string, string | undefined];
export default _default;
