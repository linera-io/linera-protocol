import * as React from 'react';
import type { SegmentedValue } from '.';
export interface MotionThumbInterface {
    containerRef: React.RefObject<HTMLDivElement>;
    value: SegmentedValue;
    getValueIndex: (value: SegmentedValue) => number;
    prefixCls: string;
    motionName: string;
    onMotionStart: VoidFunction;
    onMotionEnd: VoidFunction;
    direction?: 'ltr' | 'rtl';
}
export default function MotionThumb(props: MotionThumbInterface): React.JSX.Element | null;
