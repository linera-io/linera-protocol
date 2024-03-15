import * as React from 'react';
export interface DrawerContextProps {
    pushDistance?: number | string;
    push: VoidFunction;
    pull: VoidFunction;
}
declare const DrawerContext: React.Context<DrawerContextProps>;
export interface RefContextProps {
    panel?: React.Ref<HTMLDivElement>;
}
export declare const RefContext: React.Context<RefContextProps>;
export default DrawerContext;
