import * as React from 'react';
import type { FormatConfig, valueType } from './utils';
interface NumberProps extends FormatConfig {
    value: valueType;
    prefixCls?: string;
}
declare const StatisticNumber: React.FC<NumberProps>;
export default StatisticNumber;
