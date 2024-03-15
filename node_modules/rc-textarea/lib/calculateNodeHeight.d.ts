import type React from 'react';
export interface NodeType {
    sizingStyle: string;
    paddingSize: number;
    borderSize: number;
    boxSizing: string;
}
export declare function calculateNodeStyling(node: HTMLElement, useCache?: boolean): NodeType;
export default function calculateAutoSizeStyle(uiTextNode: HTMLTextAreaElement, useCache?: boolean, minRows?: number | null, maxRows?: number | null): React.CSSProperties;
