import * as React from 'react';
import type { InternalDescriptionsItemType } from '.';
export interface RowProps {
    prefixCls: string;
    vertical: boolean;
    row: InternalDescriptionsItemType[];
    bordered?: boolean;
    colon: boolean;
    index: number;
    children?: React.ReactNode;
}
declare const Row: React.FC<RowProps>;
export default Row;
