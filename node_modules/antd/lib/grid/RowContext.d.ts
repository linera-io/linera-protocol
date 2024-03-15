/// <reference types="react" />
export interface RowContextState {
    gutter?: [number, number];
    wrap?: boolean;
}
declare const RowContext: import("react").Context<RowContextState>;
export default RowContext;
