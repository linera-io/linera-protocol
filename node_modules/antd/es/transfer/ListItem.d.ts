import * as React from 'react';
import type { KeyWiseTransferItem } from '.';
type ListItemProps<RecordType> = {
    renderedText?: string | number;
    renderedEl: React.ReactNode;
    disabled?: boolean;
    checked?: boolean;
    prefixCls: string;
    onClick: (item: RecordType, e: React.MouseEvent<HTMLLIElement, MouseEvent>) => void;
    onRemove?: (item: RecordType) => void;
    item: RecordType;
    showRemove?: boolean;
};
declare const _default: React.MemoExoticComponent<(<RecordType extends KeyWiseTransferItem>(props: ListItemProps<RecordType>) => React.JSX.Element)>;
export default _default;
