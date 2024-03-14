import * as React from 'react';
export interface FooterRowProps {
    children?: React.ReactNode;
    className?: string;
    style?: React.CSSProperties;
    onClick?: (e?: React.MouseEvent<HTMLElement>) => void;
}
export default function FooterRow({ children, ...props }: FooterRowProps): React.JSX.Element;
