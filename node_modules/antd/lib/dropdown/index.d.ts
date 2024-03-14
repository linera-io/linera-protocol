/// <reference types="react" />
import DropdownButton from './dropdown-button';
export type { DropdownProps as DropDownProps, DropdownProps } from './dropdown';
export type { DropdownButtonProps, DropdownButtonType } from './dropdown-button';
declare const Dropdown: import("react").FC<import("./dropdown").DropdownProps> & {
    _InternalPanelDoNotUseOrYouWillBeFired: import("react").FC<import("./dropdown").DropdownProps>;
} & {
    Button: typeof DropdownButton;
};
export default Dropdown;
