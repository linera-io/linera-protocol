import type * as React from 'react';
export interface SummaryProps {
    fixed?: boolean | 'top' | 'bottom';
    children?: React.ReactNode;
}
/**
 * Syntactic sugar. Do not support HOC.
 */
declare function Summary({ children }: SummaryProps): React.ReactElement<any, string | React.JSXElementConstructor<any>>;
declare namespace Summary {
    var Row: typeof import("./Row").default;
    var Cell: typeof import("./Cell").default;
}
export default Summary;
