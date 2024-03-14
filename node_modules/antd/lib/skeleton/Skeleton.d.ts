import * as React from 'react';
import type { AvatarProps } from './Avatar';
import SkeletonAvatar from './Avatar';
import SkeletonButton from './Button';
import SkeletonImage from './Image';
import SkeletonInput from './Input';
import SkeletonNode from './Node';
import type { SkeletonParagraphProps } from './Paragraph';
import type { SkeletonTitleProps } from './Title';
type SkeletonAvatarProps = Omit<AvatarProps, 'active'>;
export interface SkeletonProps {
    active?: boolean;
    loading?: boolean;
    prefixCls?: string;
    className?: string;
    rootClassName?: string;
    style?: React.CSSProperties;
    children?: React.ReactNode;
    avatar?: SkeletonAvatarProps | boolean;
    title?: SkeletonTitleProps | boolean;
    paragraph?: SkeletonParagraphProps | boolean;
    round?: boolean;
}
type CompoundedComponent = {
    Button: typeof SkeletonButton;
    Avatar: typeof SkeletonAvatar;
    Input: typeof SkeletonInput;
    Image: typeof SkeletonImage;
    Node: typeof SkeletonNode;
};
declare const Skeleton: React.FC<SkeletonProps> & CompoundedComponent;
export default Skeleton;
