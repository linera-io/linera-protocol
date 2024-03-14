import * as React from 'react';
import type { StatisticProps } from './Statistic';
import type { valueType } from './utils';
export interface CountdownProps extends StatisticProps {
    format?: string;
    onFinish?: () => void;
    onChange?: (value?: valueType) => void;
}
declare const _default: React.NamedExoticComponent<CountdownProps>;
export default _default;
