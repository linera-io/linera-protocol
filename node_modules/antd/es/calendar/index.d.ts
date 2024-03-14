/// <reference types="react" />
import type { Dayjs } from 'dayjs';
import type { CalendarProps } from './generateCalendar';
import generateCalendar from './generateCalendar';
declare const Calendar: {
    (props: CalendarProps<Dayjs>): import("react").ReactElement<any, string | import("react").JSXElementConstructor<any>>;
    displayName: string;
};
export type CalendarType = typeof Calendar & {
    generateCalendar: typeof generateCalendar;
};
export type { CalendarProps };
declare const _default: CalendarType;
export default _default;
