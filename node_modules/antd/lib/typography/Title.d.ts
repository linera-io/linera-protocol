import * as React from 'react';
import type { BlockProps } from './Base';
declare const TITLE_ELE_LIST: readonly [1, 2, 3, 4, 5];
export interface TitleProps extends Omit<BlockProps<'h1' | 'h2' | 'h3' | 'h4' | 'h5'>, 'strong'>, Omit<React.HTMLAttributes<HTMLHeadElement>, 'type' | keyof BlockProps<'h1' | 'h2' | 'h3' | 'h4' | 'h5'>> {
    level?: typeof TITLE_ELE_LIST[number];
}
declare const Title: React.ForwardRefExoticComponent<TitleProps & React.RefAttributes<HTMLElement>>;
export default Title;
