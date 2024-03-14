import * as React from 'react';
import type { PanelMode, SharedPanelProps } from '../interface';
export interface PanelContextProps<DateType extends object = any> extends Pick<SharedPanelProps<DateType>, 'prefixCls' | 'cellRender' | 'generateConfig' | 'locale' | 'onSelect' | 'hoverValue' | 'hoverRangeValue' | 'onHover' | 'values' | 'pickerValue' | 'disabledDate' | 'minDate' | 'maxDate' | 'prevIcon' | 'nextIcon' | 'superPrevIcon' | 'superNextIcon'> {
    /** Tell current panel type */
    panelType: PanelMode;
    now: DateType;
}
/** Used for each single Panel. e.g. DatePanel */
export declare const PanelContext: React.Context<PanelContextProps<any>>;
export declare function usePanelContext<DateType extends object = any>(): PanelContextProps<DateType>;
/**
 * Get shared props for the SharedPanelProps interface.
 */
export declare function useInfo<DateType extends object = any>(props: SharedPanelProps<DateType>, panelType: PanelMode): [sharedProps: PanelContextProps<DateType>, now: DateType];
export interface PickerHackContextProps {
    hidePrev?: boolean;
    hideNext?: boolean;
    hideHeader?: boolean;
    onCellDblClick?: () => void;
}
/**
 * Internal usage for RangePicker to not to show the operation arrow
 */
export declare const PickerHackContext: React.Context<PickerHackContextProps>;
