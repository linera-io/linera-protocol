import * as React from 'react';
import type { MenuHoverEventHandler } from '../interface';
interface ActiveObj {
    active: boolean;
    onMouseEnter?: React.MouseEventHandler<HTMLElement>;
    onMouseLeave?: React.MouseEventHandler<HTMLElement>;
}
export default function useActive(eventKey: string, disabled: boolean, onMouseEnter?: MenuHoverEventHandler, onMouseLeave?: MenuHoverEventHandler): ActiveObj;
export {};
