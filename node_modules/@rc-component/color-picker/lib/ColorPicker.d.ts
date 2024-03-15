import type { CSSProperties } from 'react';
import React from 'react';
import type { BaseColorPickerProps, ColorGenInput } from './interface';
export interface ColorPickerProps extends BaseColorPickerProps {
    value?: ColorGenInput;
    defaultValue?: ColorGenInput;
    className?: string;
    style?: CSSProperties;
    /** Get panel element  */
    panelRender?: (panel: React.ReactElement) => React.ReactElement;
    /** Disabled alpha selection */
    disabledAlpha?: boolean;
}
declare const _default: React.ForwardRefExoticComponent<ColorPickerProps & React.RefAttributes<HTMLDivElement>>;
export default _default;
