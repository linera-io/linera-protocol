import * as React from 'react';
export interface CardMetaProps {
    prefixCls?: string;
    style?: React.CSSProperties;
    className?: string;
    avatar?: React.ReactNode;
    title?: React.ReactNode;
    description?: React.ReactNode;
}
declare const Meta: React.FC<CardMetaProps>;
export default Meta;
