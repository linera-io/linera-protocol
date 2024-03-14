import * as React from 'react';
import { type PickerPanelProps } from '../../PickerPanel';
import { type FooterProps } from './Footer';
export type MustProp<DateType extends object> = Required<Pick<PickerPanelProps<DateType>, 'mode' | 'onPanelChange'>>;
export type PopupPanelProps<DateType extends object = any> = MustProp<DateType> & Omit<PickerPanelProps<DateType>, 'onPickerValueChange' | 'showTime'> & FooterProps<DateType> & {
    multiplePanel?: boolean;
    range?: boolean;
    onPickerValueChange: (date: DateType) => void;
};
export default function PopupPanel<DateType extends object = any>(props: PopupPanelProps<DateType>): React.JSX.Element;
