import React from 'react';
import type { IDialogPropTypes } from '../../IDialogPropTypes';
export interface PanelProps extends Omit<IDialogPropTypes, 'getOpenCount'> {
    prefixCls: string;
    ariaId?: string;
    onMouseDown?: React.MouseEventHandler;
    onMouseUp?: React.MouseEventHandler;
    holderRef?: React.Ref<HTMLDivElement>;
}
export type ContentRef = {
    focus: () => void;
    changeActive: (next: boolean) => void;
};
declare const Panel: React.ForwardRefExoticComponent<PanelProps & React.RefAttributes<ContentRef>>;
export default Panel;
