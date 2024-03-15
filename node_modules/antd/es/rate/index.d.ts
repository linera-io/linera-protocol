import type { RateRef, RateProps as RcRateProps } from 'rc-rate/lib/Rate';
import * as React from 'react';
export interface RateProps extends RcRateProps {
    rootClassName?: string;
    tooltips?: Array<string>;
}
declare const Rate: React.ForwardRefExoticComponent<RateProps & React.RefAttributes<RateRef>>;
export default Rate;
