import * as React from 'react';
import type { CellType, ColumnType, DataIndex } from '../interface';
export default function useCellRender<RecordType>(record: RecordType, dataIndex: DataIndex, renderIndex: number, children?: React.ReactNode, render?: ColumnType<RecordType>['render'], shouldCellUpdate?: ColumnType<RecordType>['shouldCellUpdate']): [React.ReactNode, CellType<RecordType>] | [React.ReactNode];
