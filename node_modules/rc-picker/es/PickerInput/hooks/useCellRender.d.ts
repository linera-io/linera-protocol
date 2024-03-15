import type { CellRender, CellRenderInfo, SharedPickerProps } from '../../interface';
export default function useCellRender<DateType extends object = any>(cellRender: SharedPickerProps<DateType>['cellRender'], dateRender?: SharedPickerProps<DateType>['dateRender'], monthCellRender?: SharedPickerProps<DateType>['monthCellRender'], range?: CellRenderInfo<DateType>['range']): CellRender<DateType>;
