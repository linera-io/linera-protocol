import * as React from 'react';
import type { SkeletonElementProps } from './Element';
export interface SkeletonInputProps extends Omit<SkeletonElementProps, 'size' | 'shape'> {
    size?: 'large' | 'small' | 'default';
    block?: boolean;
}
declare const SkeletonInput: React.FC<SkeletonInputProps>;
export default SkeletonInput;
