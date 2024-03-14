import * as React from 'react';
export type SizeType = 'small' | 'middle' | 'large' | undefined;
declare const SizeContext: React.Context<SizeType>;
export interface SizeContextProps {
    size?: SizeType;
    children?: React.ReactNode;
}
export declare const SizeContextProvider: React.FC<SizeContextProps>;
export default SizeContext;
