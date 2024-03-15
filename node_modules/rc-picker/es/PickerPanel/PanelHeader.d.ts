import * as React from 'react';
export interface HeaderProps<DateType extends object> {
    offset?: (distance: number, date: DateType) => DateType;
    superOffset?: (distance: number, date: DateType) => DateType;
    onChange?: (date: DateType) => void;
    getStart?: (date: DateType) => DateType;
    getEnd?: (date: DateType) => DateType;
    children?: React.ReactNode;
}
declare function PanelHeader<DateType extends object>(props: HeaderProps<DateType>): React.JSX.Element;
export default PanelHeader;
