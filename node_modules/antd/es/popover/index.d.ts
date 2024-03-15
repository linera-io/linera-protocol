import * as React from 'react';
import type { RenderFunction } from '../_util/getRenderPropValue';
import type { AbstractTooltipProps } from '../tooltip';
import PurePanel from './PurePanel';
export interface PopoverProps extends AbstractTooltipProps {
    title?: React.ReactNode | RenderFunction;
    content?: React.ReactNode | RenderFunction;
}
declare const Popover: React.ForwardRefExoticComponent<PopoverProps & React.RefAttributes<unknown>> & {
    _InternalPanelDoNotUseOrYouWillBeFired: typeof PurePanel;
};
export default Popover;
