import * as React from 'react';
import type { SelectProps } from '../select';
import Select from '../select';
type CompoundedComponent = React.FC<SelectProps> & {
    Option: typeof Select.Option;
};
declare const MiniSelect: CompoundedComponent;
declare const MiddleSelect: CompoundedComponent;
export { MiniSelect, MiddleSelect };
