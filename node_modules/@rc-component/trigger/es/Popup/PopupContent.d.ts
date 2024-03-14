import * as React from 'react';
export interface PopupContentProps {
    children?: React.ReactNode;
    cache?: boolean;
}
declare const PopupContent: React.MemoExoticComponent<({ children }: PopupContentProps) => React.ReactElement<any, string | React.JSXElementConstructor<any>>>;
export default PopupContent;
