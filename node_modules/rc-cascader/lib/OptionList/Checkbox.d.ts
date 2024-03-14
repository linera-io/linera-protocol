import * as React from 'react';
export interface CheckboxProps {
    prefixCls: string;
    checked?: boolean;
    halfChecked?: boolean;
    disabled?: boolean;
    onClick?: React.MouseEventHandler;
    disableCheckbox: boolean;
}
export default function Checkbox({ prefixCls, checked, halfChecked, disabled, onClick, disableCheckbox, }: CheckboxProps): React.JSX.Element;
