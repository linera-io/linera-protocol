"use client";

import dayjsGenerateConfig from "rc-picker/es/generate/dayjs";
import generateCalendar from './generateCalendar';
const Calendar = generateCalendar(dayjsGenerateConfig);
Calendar.generateCalendar = generateCalendar;
export default Calendar;