import type * as React from 'react';
import type { CSSMotionProps } from 'rc-motion';
import type { NotificationPlacement } from './interface';
export declare function getPlacementStyle(placement: NotificationPlacement, top: number, bottom: number): React.CSSProperties;
export declare function getMotion(prefixCls: string): CSSMotionProps;
