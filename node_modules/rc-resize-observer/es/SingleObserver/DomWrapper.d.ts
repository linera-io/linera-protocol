import * as React from 'react';
export interface DomWrapperProps {
    children: React.ReactElement;
}
/**
 * Fallback to findDOMNode if origin ref do not provide any dom element
 */
export default class DomWrapper extends React.Component<DomWrapperProps> {
    render(): React.ReactElement<any, string | React.JSXElementConstructor<any>> & React.ReactNode;
}
