import * as React from 'react';
import type { BlockProps } from './Base';
export interface LinkProps extends BlockProps<'a'>, Omit<React.AnchorHTMLAttributes<HTMLAnchorElement>, 'type' | keyof BlockProps<'a'>> {
    ellipsis?: boolean;
}
declare const Link: React.ForwardRefExoticComponent<LinkProps & React.RefAttributes<HTMLElement>>;
export default Link;
