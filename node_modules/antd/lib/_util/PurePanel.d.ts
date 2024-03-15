import * as React from 'react';
import type { AnyObject } from './type';
export declare function withPureRenderTheme<T extends AnyObject = AnyObject>(Component: React.FC<T>): (props: T) => React.JSX.Element;
export interface BaseProps {
    prefixCls?: string;
    style?: React.CSSProperties;
}
declare const genPurePanel: <ComponentProps extends BaseProps = BaseProps>(Component: any, defaultPrefixCls?: string, getDropdownCls?: ((prefixCls: string) => string) | null | undefined, postProps?: ((props: ComponentProps) => ComponentProps) | undefined) => (props: AnyObject) => React.JSX.Element;
export default genPurePanel;
