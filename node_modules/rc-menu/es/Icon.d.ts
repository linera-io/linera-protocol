import * as React from 'react';
import type { RenderIconInfo, RenderIconType } from './interface';
export interface IconProps {
    icon?: RenderIconType;
    props: RenderIconInfo;
    /** Fallback of icon if provided */
    children?: React.ReactElement;
}
export default function Icon({ icon, props, children }: IconProps): React.ReactElement<any, string | React.JSXElementConstructor<any>>;
