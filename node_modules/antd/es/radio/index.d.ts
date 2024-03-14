import type * as React from 'react';
import Group from './group';
import type { RadioProps, RadioRef } from './interface';
import Button from './radioButton';
export { RadioChangeEvent, RadioChangeEventTarget, RadioGroupButtonStyle, RadioGroupContextProps, RadioGroupOptionType, RadioGroupProps, RadioProps, RadioRef, } from './interface';
export { Button, Group };
type CompoundedComponent = React.ForwardRefExoticComponent<RadioProps & React.RefAttributes<RadioRef>> & {
    Group: typeof Group;
    Button: typeof Button;
};
declare const Radio: CompoundedComponent;
export default Radio;
