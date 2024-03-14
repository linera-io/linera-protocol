import * as React from 'react';
import type { OnGroupPreview, RegisterImage } from './interface';
export interface PreviewGroupContextProps {
    register: RegisterImage;
    onPreview: OnGroupPreview;
}
export declare const PreviewGroupContext: React.Context<PreviewGroupContextProps>;
