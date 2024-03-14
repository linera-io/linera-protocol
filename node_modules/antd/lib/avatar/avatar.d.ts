import * as React from 'react';
import type { AvatarSize } from './AvatarContext';
export interface AvatarProps {
    /** Shape of avatar, options: `circle`, `square` */
    shape?: 'circle' | 'square';
    size?: AvatarSize;
    gap?: number;
    /** Src of image avatar */
    src?: React.ReactNode;
    /** Srcset of image avatar */
    srcSet?: string;
    draggable?: boolean | 'true' | 'false';
    /** Icon to be used in avatar */
    icon?: React.ReactNode;
    style?: React.CSSProperties;
    prefixCls?: string;
    className?: string;
    rootClassName?: string;
    children?: React.ReactNode;
    alt?: string;
    crossOrigin?: '' | 'anonymous' | 'use-credentials';
    onClick?: (e?: React.MouseEvent<HTMLElement>) => void;
    onError?: () => boolean;
}
declare const Avatar: React.ForwardRefExoticComponent<AvatarProps & React.RefAttributes<HTMLSpanElement>>;
export default Avatar;
