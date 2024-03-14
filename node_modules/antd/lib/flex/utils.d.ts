import type { FlexProps } from './interface';
export declare const flexWrapValues: readonly ["wrap", "nowrap", "wrap-reverse"];
export declare const justifyContentValues: readonly ["flex-start", "flex-end", "start", "end", "center", "space-between", "space-around", "space-evenly", "stretch", "normal", "left", "right"];
export declare const alignItemsValues: readonly ["center", "start", "end", "flex-start", "flex-end", "self-start", "self-end", "baseline", "normal", "stretch"];
declare function createFlexClassNames(prefixCls: string, props: FlexProps): string;
export default createFlexClassNames;
