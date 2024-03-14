import * as React from 'react';
import type { TableLocale } from './interface';
interface DefaultExpandIconProps<RecordType> {
    prefixCls: string;
    onExpand: (record: RecordType, e: React.MouseEvent<HTMLElement>) => void;
    record: RecordType;
    expanded: boolean;
    expandable: boolean;
}
declare function renderExpandIcon(locale: TableLocale): <RecordType>({ prefixCls, onExpand, record, expanded, expandable, }: DefaultExpandIconProps<RecordType>) => React.JSX.Element;
export default renderExpandIcon;
