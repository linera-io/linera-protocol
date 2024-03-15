import * as React from 'react';
export interface TitleProps {
    className: string;
    children: React.ReactNode;
}
declare function Panel({ className, children }: TitleProps): React.JSX.Element;
export default Panel;
