import * as React from 'react';
import type { TriggerProps } from '.';
export interface TriggerWrapperProps {
    getTriggerDOMNode?: TriggerProps['getTriggerDOMNode'];
    children: React.ReactElement;
}
declare const TriggerWrapper: React.ForwardRefExoticComponent<TriggerWrapperProps & React.RefAttributes<HTMLElement>>;
export default TriggerWrapper;
