import React from 'react';
import type { DropdownProps } from './Dropdown';
export type OverlayProps = Pick<DropdownProps, 'overlay' | 'arrow' | 'prefixCls'>;
declare const Overlay: React.ForwardRefExoticComponent<OverlayProps & React.RefAttributes<HTMLElement>>;
export default Overlay;
