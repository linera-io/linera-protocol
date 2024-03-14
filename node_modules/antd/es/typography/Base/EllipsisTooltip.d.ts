import * as React from 'react';
import type { TooltipProps } from '../../tooltip';
export interface EllipsisTooltipProps {
    tooltipProps?: TooltipProps;
    enableEllipsis: boolean;
    isEllipsis?: boolean;
    children: React.ReactElement;
}
declare const EllipsisTooltip: React.FC<EllipsisTooltipProps>;
export default EllipsisTooltip;
