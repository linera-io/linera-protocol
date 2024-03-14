import * as React from 'react';
import type { Settings } from '@ant-design/react-slick';
export type CarouselEffect = 'scrollx' | 'fade';
export type DotPosition = 'top' | 'bottom' | 'left' | 'right';
export interface CarouselProps extends Omit<Settings, 'dots' | 'dotsClass'> {
    effect?: CarouselEffect;
    style?: React.CSSProperties;
    prefixCls?: string;
    rootClassName?: string;
    id?: string;
    slickGoTo?: number;
    dotPosition?: DotPosition;
    children?: React.ReactNode;
    dots?: boolean | {
        className?: string;
    };
    waitForAnimate?: boolean;
}
export interface CarouselRef {
    goTo: (slide: number, dontAnimate?: boolean) => void;
    next: () => void;
    prev: () => void;
    autoPlay: (palyType?: 'update' | 'leave' | 'blur') => void;
    innerSlider: any;
}
declare const Carousel: React.ForwardRefExoticComponent<CarouselProps & React.RefAttributes<CarouselRef>>;
export default Carousel;
