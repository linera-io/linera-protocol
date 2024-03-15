import React from 'react';
import type { SliderProps as RcSliderProps } from 'rc-slider';
import type { SliderRef } from 'rc-slider/lib/Slider';
import type { AbstractTooltipProps, TooltipPlacement } from '../tooltip';
export type SliderMarks = RcSliderProps['marks'];
interface HandleGeneratorInfo {
    value?: number;
    dragging?: boolean;
    index: number;
}
export type HandleGeneratorFn = (config: {
    tooltipPrefixCls?: string;
    prefixCls?: string;
    info: HandleGeneratorInfo;
}) => React.ReactElement;
export type Formatter = ((value?: number) => React.ReactNode) | null;
export interface SliderTooltipProps extends AbstractTooltipProps {
    prefixCls?: string;
    open?: boolean;
    placement?: TooltipPlacement;
    getPopupContainer?: (triggerNode: HTMLElement) => HTMLElement;
    formatter?: Formatter;
    autoAdjustOverflow?: boolean;
}
export interface SliderBaseProps {
    prefixCls?: string;
    reverse?: boolean;
    min?: number;
    max?: number;
    step?: null | number;
    marks?: SliderMarks;
    dots?: boolean;
    included?: boolean;
    disabled?: boolean;
    keyboard?: boolean;
    vertical?: boolean;
    className?: string;
    rootClassName?: string;
    id?: string;
    style?: React.CSSProperties;
    tooltip?: SliderTooltipProps;
    autoFocus?: boolean;
    styles?: RcSliderProps['styles'];
    classNames?: RcSliderProps['classNames'];
    onFocus?: React.FocusEventHandler<HTMLDivElement>;
    onBlur?: React.FocusEventHandler<HTMLDivElement>;
    /** @deprecated `tooltipPrefixCls` is deprecated. Please use `tooltip.prefixCls` instead. */
    tooltipPrefixCls?: string;
    /** @deprecated `tipFormatter` is deprecated. Please use `tooltip.formatter` instead. */
    tipFormatter?: Formatter;
    /** @deprecated `tooltipVisible` is deprecated. Please use `tooltip.open` instead. */
    tooltipVisible?: boolean;
    /**
     * @deprecated `getTooltipPopupContainer` is deprecated. Please use `tooltip.getPopupContainer`
     *   instead.
     */
    getTooltipPopupContainer?: (triggerNode: HTMLElement) => HTMLElement;
    /** @deprecated `tooltipPlacement` is deprecated. Please use `tooltip.placement` instead. */
    tooltipPlacement?: TooltipPlacement;
}
export interface SliderSingleProps extends SliderBaseProps {
    range?: false;
    value?: number;
    defaultValue?: number;
    onChange?: (value: number) => void;
    /** @deprecated Please use `onChangeComplete` instead */
    onAfterChange?: (value: number) => void;
    onChangeComplete?: (value: number) => void;
    /** @deprecated Please use `styles.handle` instead */
    handleStyle?: React.CSSProperties;
    /** @deprecated Please use `styles.track` instead */
    trackStyle?: React.CSSProperties;
    /** @deprecated Please use `styles.rail` instead */
    railStyle?: React.CSSProperties;
}
export interface SliderRangeProps extends SliderBaseProps {
    range: true | SliderRange;
    value?: number[];
    defaultValue?: number[];
    onChange?: (value: number[]) => void;
    /** @deprecated Please use `onChangeComplete` instead */
    onAfterChange?: (value: number[]) => void;
    onChangeComplete?: (value: number[]) => void;
    /** @deprecated Please use `styles.handle` instead */
    handleStyle?: React.CSSProperties[];
    /** @deprecated Please use `styles.track` instead */
    trackStyle?: React.CSSProperties[];
    /** @deprecated Please use `styles.rail` instead */
    railStyle?: React.CSSProperties;
}
interface SliderRange {
    draggableTrack?: boolean;
}
export type Opens = {
    [index: number]: boolean;
};
declare const Slider: React.ForwardRefExoticComponent<(SliderSingleProps | SliderRangeProps) & React.RefAttributes<SliderRef>>;
export default Slider;
