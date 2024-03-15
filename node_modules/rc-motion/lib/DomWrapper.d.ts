import * as React from 'react';
export interface DomWrapperProps {
    children: React.ReactNode;
}
declare class DomWrapper extends React.Component<DomWrapperProps> {
    render(): React.ReactNode;
}
export default DomWrapper;
