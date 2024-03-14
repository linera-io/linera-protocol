import * as React from 'react';
export interface TransferLocale {
    description: string;
}
export interface EmptyProps {
    prefixCls?: string;
    className?: string;
    rootClassName?: string;
    style?: React.CSSProperties;
    /** @since 3.16.0 */
    imageStyle?: React.CSSProperties;
    image?: React.ReactNode;
    description?: React.ReactNode;
    children?: React.ReactNode;
}
type CompoundedComponent = React.FC<EmptyProps> & {
    PRESENTED_IMAGE_DEFAULT: React.ReactNode;
    PRESENTED_IMAGE_SIMPLE: React.ReactNode;
};
declare const Empty: CompoundedComponent;
export default Empty;
