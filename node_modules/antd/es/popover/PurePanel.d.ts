import * as React from 'react';
import type { PopoverProps } from '.';
export declare const getOverlay: (prefixCls?: string, title?: PopoverProps['title'], content?: PopoverProps['content']) => React.JSX.Element | null;
export interface PurePanelProps extends Omit<PopoverProps, 'children'> {
    children?: React.ReactNode;
}
interface RawPurePanelProps extends PopoverProps {
    hashId: string;
}
export declare const RawPurePanel: React.FC<RawPurePanelProps>;
declare const PurePanel: React.FC<PurePanelProps>;
export default PurePanel;
