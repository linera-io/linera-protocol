import type { CountdownProps } from './Countdown';
import Countdown from './Countdown';
import type { StatisticProps } from './Statistic';
import Statistic from './Statistic';
export type { CountdownProps, StatisticProps };
type CompoundedComponent = {
    Countdown: typeof Countdown;
};
export type CompoundedStatistic = typeof Statistic & CompoundedComponent;
declare const _default: CompoundedStatistic;
export default _default;
