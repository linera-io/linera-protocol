import React from 'react';
import type { AnyObject } from './type';
export declare function isFragment(child: any): boolean;
type RenderProps = AnyObject | ((originProps: AnyObject) => AnyObject | void);
export declare const replaceElement: <P>(element: React.ReactNode, replacement: React.ReactNode, props?: RenderProps) => React.ReactNode;
export declare function cloneElement<P>(element: React.ReactNode, props?: RenderProps): React.ReactElement<any, string | React.JSXElementConstructor<any>>;
export {};
