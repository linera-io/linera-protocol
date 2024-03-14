import * as React from 'react';
export interface IconProps extends React.HtmlHTMLAttributes<HTMLElement> {
    icon?: React.ReactNode;
    type: 'suffix' | 'clear';
}
export default function Icon(props: IconProps): React.JSX.Element;
export interface ClearIconProps extends Omit<IconProps, 'type'> {
    onClear: VoidFunction;
}
export declare function ClearIcon({ onClear, ...restProps }: ClearIconProps): React.JSX.Element;
