import type * as React from 'react';
export type valueType = number | string;
export type countdownValueType = number | string;
export type Formatter = false | 'number' | 'countdown' | ((value: valueType, config?: FormatConfig) => React.ReactNode);
export interface FormatConfig {
    formatter?: Formatter;
    decimalSeparator?: string;
    groupSeparator?: string;
    precision?: number;
}
export interface CountdownFormatConfig extends FormatConfig {
    format?: string;
}
export declare function formatTimeStr(duration: number, format: string): string;
export declare function formatCountdown(value: valueType, config: CountdownFormatConfig): string;
