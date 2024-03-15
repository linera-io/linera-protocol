import * as React from 'react';
import type { PanelProps, ContentRef } from './Panel';
export type ContentProps = {
    motionName: string;
    ariaId: string;
    onVisibleChanged: (visible: boolean) => void;
} & PanelProps;
declare const Content: React.ForwardRefExoticComponent<{
    motionName: string;
    ariaId: string;
    onVisibleChanged: (visible: boolean) => void;
} & PanelProps & React.RefAttributes<ContentRef>>;
export default Content;
