import * as React from 'react';
type widthUnit = number | string;
export interface SkeletonParagraphProps {
    prefixCls?: string;
    className?: string;
    style?: React.CSSProperties;
    width?: widthUnit | Array<widthUnit>;
    rows?: number;
}
declare const Paragraph: React.FC<SkeletonParagraphProps>;
export default Paragraph;
