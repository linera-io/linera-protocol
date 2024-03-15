/// <reference types="react" />
import type { GetDefaultToken } from '../../theme/internal';
import type { TreeSharedToken } from '../../tree/style';
export interface ComponentToken extends TreeSharedToken {
}
export declare const prepareComponentToken: GetDefaultToken<'TreeSelect'>;
export default function useTreeSelectStyle(prefixCls: string, treePrefixCls: string, rootCls: string): readonly [(node: import("react").ReactElement<any, string | import("react").JSXElementConstructor<any>>) => import("react").ReactElement<any, string | import("react").JSXElementConstructor<any>>, string, string | undefined];
