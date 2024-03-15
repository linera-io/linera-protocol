import type { GenerateConfig } from '../../../generate';
import type { Unit } from './TimeColumn';
export declare function findValidateTime<DateType>(date: DateType, getHourUnits: () => Unit<number>[], getMinuteUnits: (hour: number) => Unit<number>[], getSecondUnits: (hour: number, minute: number) => Unit<number>[], getMillisecondUnits: (hour: number, minute: number, second: number) => Unit<number>[], generateConfig: GenerateConfig<DateType>): DateType;
