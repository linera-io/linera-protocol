import type { TablePaginationConfig } from '../interface';
export declare const DEFAULT_PAGE_SIZE = 10;
export declare function getPaginationParam(mergedPagination: TablePaginationConfig, pagination?: TablePaginationConfig | boolean): any;
declare function usePagination(total: number, onChange: (current: number, pageSize: number) => void, pagination?: TablePaginationConfig | false): readonly [TablePaginationConfig, (current?: number, pageSize?: number) => void];
export default usePagination;
