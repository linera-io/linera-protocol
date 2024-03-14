import type * as React from 'react';
import type { DefaultOptionType } from './Select';
export interface OptionProps extends Omit<DefaultOptionType, 'label'> {
    children: React.ReactNode;
    /** Save for customize data */
    [prop: string]: any;
}
export interface OptionFC extends React.FC<OptionProps> {
    /** Legacy for check if is a Option Group */
    isSelectOption: boolean;
}
/** This is a placeholder, not real render in dom */
declare const Option: OptionFC;
export default Option;
