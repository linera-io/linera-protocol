import React from 'react';
interface DomWrapperProps {
    children: React.ReactNode;
}
declare class DomWrapper extends React.Component<DomWrapperProps> {
    render(): React.ReactNode;
}
export default DomWrapper;
