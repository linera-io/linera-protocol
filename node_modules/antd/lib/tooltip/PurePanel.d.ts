import * as React from 'react';
import type { TooltipProps } from '.';
export interface PurePanelProps extends Omit<TooltipProps, 'children'> {
}
/** @private Internal Component. Do not use in your production. */
declare const PurePanel: React.FC<PurePanelProps>;
export default PurePanel;
