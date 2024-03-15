import * as React from 'react';
export type ComponentType = React.ComponentType<any> | React.ForwardRefExoticComponent<any> | React.FC<any> | keyof React.ReactHTML;
export interface RawItemProps extends React.HTMLAttributes<any> {
    component?: ComponentType;
    children?: React.ReactNode;
}
declare const RawItem: React.ForwardRefExoticComponent<RawItemProps & React.RefAttributes<any>>;
export default RawItem;
