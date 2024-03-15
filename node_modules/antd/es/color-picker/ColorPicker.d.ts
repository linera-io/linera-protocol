import type { CSSProperties, FC } from 'react';
import React from 'react';
import type { ColorPickerProps as RcColorPickerProps } from '@rc-component/color-picker';
import type { SizeType } from '../config-provider/SizeContext';
import type { PopoverProps } from '../popover';
import type { Color } from './color';
import type { ColorFormat, ColorValueType, PresetsItem, TriggerPlacement, TriggerType } from './interface';
export type ColorPickerProps = Omit<RcColorPickerProps, 'onChange' | 'value' | 'defaultValue' | 'panelRender' | 'disabledAlpha' | 'onChangeComplete'> & {
    value?: ColorValueType;
    defaultValue?: ColorValueType;
    children?: React.ReactNode;
    open?: boolean;
    disabled?: boolean;
    placement?: TriggerPlacement;
    trigger?: TriggerType;
    format?: keyof typeof ColorFormat;
    defaultFormat?: keyof typeof ColorFormat;
    allowClear?: boolean;
    presets?: PresetsItem[];
    arrow?: boolean | {
        pointAtCenter: boolean;
    };
    panelRender?: (panel: React.ReactNode, extra: {
        components: {
            Picker: FC;
            Presets: FC;
        };
    }) => React.ReactNode;
    showText?: boolean | ((color: Color) => React.ReactNode);
    size?: SizeType;
    styles?: {
        popup?: CSSProperties;
        popupOverlayInner?: CSSProperties;
    };
    rootClassName?: string;
    disabledAlpha?: boolean;
    [key: `data-${string}`]: string;
    onOpenChange?: (open: boolean) => void;
    onFormatChange?: (format: ColorFormat) => void;
    onChange?: (value: Color, hex: string) => void;
    onClear?: () => void;
    onChangeComplete?: (value: Color) => void;
} & Pick<PopoverProps, 'getPopupContainer' | 'autoAdjustOverflow' | 'destroyTooltipOnHide'>;
type CompoundedComponent = React.FC<ColorPickerProps> & {
    _InternalPanelDoNotUseOrYouWillBeFired: typeof PurePanel;
};
declare const ColorPicker: CompoundedComponent;
declare const PurePanel: (props: import("../_util/type").AnyObject) => React.JSX.Element;
export default ColorPicker;
