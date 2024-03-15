import * as React from 'react';
import type { EditableConfig, TabsLocale } from '../interface';
export interface AddButtonProps {
    prefixCls: string;
    editable?: EditableConfig;
    locale?: TabsLocale;
    style?: React.CSSProperties;
}
declare const AddButton: React.ForwardRefExoticComponent<AddButtonProps & React.RefAttributes<HTMLButtonElement>>;
export default AddButton;
