import * as React from 'react';
import type { MenuTheme } from './MenuContext';
interface TitleEventEntity {
    key: string;
    domEvent: React.MouseEvent<HTMLElement> | React.KeyboardEvent<HTMLElement>;
}
export interface SubMenuProps {
    className?: string;
    disabled?: boolean;
    level?: number;
    title?: React.ReactNode;
    icon?: React.ReactNode;
    style?: React.CSSProperties;
    onTitleClick?: (e: TitleEventEntity) => void;
    onTitleMouseEnter?: (e: TitleEventEntity) => void;
    onTitleMouseLeave?: (e: TitleEventEntity) => void;
    popupOffset?: [number, number];
    popupClassName?: string;
    children?: React.ReactNode;
    theme?: MenuTheme;
}
declare const SubMenu: React.FC<SubMenuProps>;
export default SubMenu;
