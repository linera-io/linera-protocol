import type * as React from 'react';
export interface OptionProps {
    value?: string;
    key?: string;
    disabled?: boolean;
    children?: React.ReactNode;
    className?: string;
    style?: React.CSSProperties;
}
declare const Option: React.FC<OptionProps>;
export default Option;
