import * as React from 'react';
import type { SkeletonElementProps } from './Element';
export interface AvatarProps extends Omit<SkeletonElementProps, 'shape'> {
    shape?: 'circle' | 'square';
}
declare const SkeletonAvatar: React.FC<AvatarProps>;
export default SkeletonAvatar;
