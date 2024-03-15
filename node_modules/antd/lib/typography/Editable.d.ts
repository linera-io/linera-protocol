import type { AutoSizeType } from 'rc-textarea';
import * as React from 'react';
import type { DirectionType } from '../config-provider';
interface EditableProps {
    prefixCls: string;
    value: string;
    ['aria-label']?: string;
    onSave: (value: string) => void;
    onCancel: () => void;
    onEnd?: () => void;
    className?: string;
    style?: React.CSSProperties;
    direction?: DirectionType;
    maxLength?: number;
    autoSize?: boolean | AutoSizeType;
    enterIcon?: React.ReactNode;
    component?: string;
}
declare const Editable: React.FC<EditableProps>;
export default Editable;
