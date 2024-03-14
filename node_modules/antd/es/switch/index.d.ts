import * as React from 'react';
export type SwitchSize = 'small' | 'default';
export type SwitchChangeEventHandler = (checked: boolean, event: React.MouseEvent<HTMLButtonElement>) => void;
export type SwitchClickEventHandler = SwitchChangeEventHandler;
export interface SwitchProps {
    prefixCls?: string;
    size?: SwitchSize;
    className?: string;
    rootClassName?: string;
    checked?: boolean;
    defaultChecked?: boolean;
    /**
     * Alias for `checked`.
     * @since 5.12.0
     */
    value?: boolean;
    /**
     * Alias for `defaultChecked`.
     * @since 5.12.0
     */
    defaultValue?: boolean;
    onChange?: SwitchChangeEventHandler;
    onClick?: SwitchClickEventHandler;
    checkedChildren?: React.ReactNode;
    unCheckedChildren?: React.ReactNode;
    disabled?: boolean;
    loading?: boolean;
    autoFocus?: boolean;
    style?: React.CSSProperties;
    title?: string;
    tabIndex?: number;
    id?: string;
}
type CompoundedComponent = React.ForwardRefExoticComponent<SwitchProps & React.RefAttributes<HTMLElement>> & {};
declare const Switch: CompoundedComponent;
export default Switch;
