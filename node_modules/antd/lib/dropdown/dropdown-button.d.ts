import * as React from 'react';
import type { ButtonProps, ButtonHTMLType } from '../button';
import type { ButtonGroupProps } from '../button/button-group';
import type { DropdownProps } from './dropdown';
export type DropdownButtonType = 'default' | 'primary' | 'dashed' | 'link' | 'text';
export interface DropdownButtonProps extends ButtonGroupProps, DropdownProps {
    type?: DropdownButtonType;
    htmlType?: ButtonHTMLType;
    danger?: boolean;
    disabled?: boolean;
    loading?: ButtonProps['loading'];
    onClick?: React.MouseEventHandler<HTMLElement>;
    icon?: React.ReactNode;
    href?: string;
    children?: React.ReactNode;
    title?: string;
    buttonsRender?: (buttons: React.ReactNode[]) => React.ReactNode[];
}
type CompoundedComponent = React.FC<DropdownButtonProps> & {};
declare const DropdownButton: CompoundedComponent;
export default DropdownButton;
