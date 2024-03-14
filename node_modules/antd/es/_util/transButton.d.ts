import * as React from 'react';
interface TransButtonProps extends React.HTMLAttributes<HTMLDivElement> {
    onClick?: (e?: React.MouseEvent<HTMLDivElement>) => void;
    noStyle?: boolean;
    autoFocus?: boolean;
    disabled?: boolean;
}
declare const TransButton: React.ForwardRefExoticComponent<TransButtonProps & React.RefAttributes<HTMLDivElement>>;
export default TransButton;
