import * as React from 'react';
import type { AbstractCheckboxProps } from '../checkbox/Checkbox';
import type { RadioChangeEvent, RadioRef } from './interface';
export type RadioButtonProps = AbstractCheckboxProps<RadioChangeEvent>;
declare const _default: React.ForwardRefExoticComponent<RadioButtonProps & React.RefAttributes<RadioRef>>;
export default _default;
