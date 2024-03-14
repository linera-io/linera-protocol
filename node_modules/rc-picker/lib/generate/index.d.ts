export type GenerateConfig<DateType> = {
    getWeekDay: (value: DateType) => number;
    getMillisecond: (value: DateType) => number;
    getSecond: (value: DateType) => number;
    getMinute: (value: DateType) => number;
    getHour: (value: DateType) => number;
    getDate: (value: DateType) => number;
    getMonth: (value: DateType) => number;
    getYear: (value: DateType) => number;
    getNow: () => DateType;
    getFixedDate: (fixed: string) => DateType;
    getEndDate: (value: DateType) => DateType;
    addYear: (value: DateType, diff: number) => DateType;
    addMonth: (value: DateType, diff: number) => DateType;
    addDate: (value: DateType, diff: number) => DateType;
    setYear: (value: DateType, year: number) => DateType;
    setMonth: (value: DateType, month: number) => DateType;
    setDate: (value: DateType, date: number) => DateType;
    setHour: (value: DateType, hour: number) => DateType;
    setMinute: (value: DateType, minute: number) => DateType;
    setSecond: (value: DateType, second: number) => DateType;
    setMillisecond: (value: DateType, millisecond: number) => DateType;
    isAfter: (date1: DateType, date2: DateType) => boolean;
    isValidate: (date: DateType) => boolean;
    locale: {
        getWeekFirstDay: (locale: string) => number;
        getWeekFirstDate: (locale: string, value: DateType) => DateType;
        getWeek: (locale: string, value: DateType) => number;
        format: (locale: string, date: DateType, format: string) => string;
        /** Should only return validate date instance */
        parse: (locale: string, text: string, formats: string[]) => DateType | null;
        /** A proxy for getting locale with moment or other locale library */
        getShortWeekDays?: (locale: string) => string[];
        /** A proxy for getting locale with moment or other locale library */
        getShortMonths?: (locale: string) => string[];
    };
};
