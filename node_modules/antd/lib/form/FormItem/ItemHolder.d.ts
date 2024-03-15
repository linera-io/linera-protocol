import * as React from 'react';
import type { Meta } from 'rc-field-form/lib/interface';
import type { FormItemProps } from '.';
import type { ReportMetaChange } from '../context';
export interface ItemHolderProps extends FormItemProps {
    prefixCls: string;
    className?: string;
    rootClassName?: string;
    style?: React.CSSProperties;
    errors: React.ReactNode[];
    warnings: React.ReactNode[];
    meta: Meta;
    children?: React.ReactNode;
    fieldId?: string;
    isRequired?: boolean;
    onSubItemMetaChange: ReportMetaChange;
}
export default function ItemHolder(props: ItemHolderProps): React.JSX.Element;
