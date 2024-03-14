import * as React from 'react';
interface MotionContextProps {
    motion?: boolean;
}
export declare const Context: React.Context<MotionContextProps>;
export default function MotionProvider({ children, ...props }: MotionContextProps & {
    children?: React.ReactNode;
}): React.JSX.Element;
export {};
