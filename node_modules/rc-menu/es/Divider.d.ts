import * as React from 'react';
import type { MenuDividerType } from './interface';
export type DividerProps = Omit<MenuDividerType, 'type'>;
export default function Divider({ className, style }: DividerProps): React.JSX.Element;
