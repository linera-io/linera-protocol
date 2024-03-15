import * as React from 'react';
export type MemoChildrenProps = {
    shouldUpdate: boolean;
    children: React.ReactNode;
};
declare const _default: React.MemoExoticComponent<({ children }: MemoChildrenProps) => React.ReactElement<any, string | React.JSXElementConstructor<any>>>;
export default _default;
