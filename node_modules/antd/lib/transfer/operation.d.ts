import * as React from 'react';
import type { DirectionType } from '../config-provider';
export interface TransferOperationProps {
    className?: string;
    leftArrowText?: string;
    rightArrowText?: string;
    moveToLeft?: React.MouseEventHandler<HTMLButtonElement | HTMLAnchorElement>;
    moveToRight?: React.MouseEventHandler<HTMLButtonElement | HTMLAnchorElement>;
    leftActive?: boolean;
    rightActive?: boolean;
    style?: React.CSSProperties;
    disabled?: boolean;
    direction?: DirectionType;
    oneWay?: boolean;
}
declare const Operation: React.FC<TransferOperationProps>;
export default Operation;
