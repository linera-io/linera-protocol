import * as React from 'react';
import type { SkeletonElementProps } from './Element';
export interface SkeletonNodeProps extends Omit<SkeletonElementProps, 'size' | 'shape'> {
    fullSize?: boolean;
    children?: React.ReactNode;
}
declare const SkeletonNode: React.FC<SkeletonNodeProps>;
export default SkeletonNode;
