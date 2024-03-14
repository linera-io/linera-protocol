import * as React from 'react';
export interface GeneratorProps {
    suffixCls?: string;
    tagName: 'header' | 'footer' | 'main' | 'div';
    displayName: string;
}
export interface BasicProps extends React.HTMLAttributes<HTMLDivElement> {
    prefixCls?: string;
    suffixCls?: string;
    rootClassName?: string;
    hasSider?: boolean;
}
declare const Layout: React.ForwardRefExoticComponent<BasicProps & React.RefAttributes<HTMLElement>>;
declare const Header: React.ForwardRefExoticComponent<BasicProps & React.RefAttributes<HTMLElement>>;
declare const Footer: React.ForwardRefExoticComponent<BasicProps & React.RefAttributes<HTMLElement>>;
declare const Content: React.ForwardRefExoticComponent<BasicProps & React.RefAttributes<HTMLElement>>;
export { Content, Footer, Header };
export default Layout;
