import * as React from 'react';
export type DisabledType = boolean | undefined;
declare const DisabledContext: React.Context<DisabledType>;
export interface DisabledContextProps {
    disabled?: DisabledType;
    children?: React.ReactNode;
}
export declare const DisabledContextProvider: React.FC<DisabledContextProps>;
export default DisabledContext;
