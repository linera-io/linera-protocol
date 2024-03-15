import * as React from 'react';
export interface InlineSubMenuListProps {
    id?: string;
    open: boolean;
    keyPath: string[];
    children: React.ReactNode;
}
export default function InlineSubMenuList({ id, open, keyPath, children, }: InlineSubMenuListProps): React.JSX.Element;
