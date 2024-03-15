import * as React from 'react';
import type { ImageProps } from 'rc-image';
import PreviewGroup from './PreviewGroup';
export interface CompositionImage<P> extends React.FC<P> {
    PreviewGroup: typeof PreviewGroup;
}
declare const Image: CompositionImage<ImageProps>;
export type { ImageProps };
export default Image;
