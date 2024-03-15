import * as React from 'react';
import type { PanelProps } from 'rc-dialog/lib/Dialog/Content/Panel';
import type { ModalFuncProps } from './interface';
export interface PurePanelProps extends Omit<PanelProps, 'prefixCls' | 'footer'>, Pick<ModalFuncProps, 'type' | 'footer'> {
    prefixCls?: string;
    style?: React.CSSProperties;
}
declare const _default: (props: PurePanelProps) => React.JSX.Element;
export default _default;
