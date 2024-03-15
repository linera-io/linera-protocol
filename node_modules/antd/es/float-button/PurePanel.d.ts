import * as React from 'react';
import type { FloatButtonGroupProps, FloatButtonProps } from './interface';
export interface PureFloatButtonProps extends Omit<FloatButtonProps, 'target'> {
    backTop?: boolean;
}
export interface PurePanelProps extends PureFloatButtonProps, Omit<FloatButtonGroupProps, 'children'> {
    /** Convert to FloatGroup when configured */
    items?: PureFloatButtonProps[];
}
/** @private Internal Component. Do not use in your production. */
declare const PurePanel: React.FC<PurePanelProps>;
export default PurePanel;
