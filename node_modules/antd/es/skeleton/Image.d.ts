import * as React from 'react';
import type { SkeletonElementProps } from './Element';
export interface SkeletonImageProps extends Omit<SkeletonElementProps, 'size' | 'shape'> {
}
declare const SkeletonImage: React.FC<SkeletonImageProps>;
export default SkeletonImage;
