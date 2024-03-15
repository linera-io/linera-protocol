import type { GenerateConfig } from 'rc-picker/lib/generate';
import type { Locale } from 'rc-picker/lib/interface';
import * as React from 'react';
import type { CalendarMode, SelectInfo } from './generateCalendar';
export interface CalendarHeaderProps<DateType> {
    prefixCls: string;
    value: DateType;
    validRange?: [DateType, DateType];
    generateConfig: GenerateConfig<DateType>;
    locale: Locale;
    mode: CalendarMode;
    fullscreen: boolean;
    onChange: (date: DateType, source: SelectInfo['source']) => void;
    onModeChange: (mode: CalendarMode) => void;
}
declare function CalendarHeader<DateType>(props: CalendarHeaderProps<DateType>): React.JSX.Element;
export default CalendarHeader;
