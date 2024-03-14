import type * as React from 'react';
import type { CheckboxRef } from 'rc-checkbox';
import type { CheckboxProps } from './Checkbox';
import Group from './Group';
export type { CheckboxChangeEvent, CheckboxProps } from './Checkbox';
export type { CheckboxGroupProps, CheckboxOptionType } from './Group';
export type { CheckboxRef };
type CompoundedComponent = React.ForwardRefExoticComponent<CheckboxProps & React.RefAttributes<CheckboxRef>> & {
    Group: typeof Group;
};
declare const Checkbox: CompoundedComponent;
export default Checkbox;
