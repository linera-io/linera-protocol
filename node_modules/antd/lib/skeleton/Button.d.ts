import * as React from 'react';
import type { SkeletonElementProps } from './Element';
export interface SkeletonButtonProps extends Omit<SkeletonElementProps, 'size'> {
    size?: 'large' | 'small' | 'default';
    block?: boolean;
}
declare const SkeletonButton: React.FC<SkeletonButtonProps>;
export default SkeletonButton;
