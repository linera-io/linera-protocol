import type { FC } from 'react';
import React from 'react';
import type { Color } from '../color';
import type { HsbaColorType } from '../interface';
declare const Gradient: FC<{
    colors: (Color | string)[];
    direction?: string;
    children?: React.ReactElement;
    type?: HsbaColorType;
    prefixCls?: string;
}>;
export default Gradient;
