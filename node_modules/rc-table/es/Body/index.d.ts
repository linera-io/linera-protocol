import * as React from 'react';
export interface BodyProps<RecordType> {
    data: readonly RecordType[];
    measureColumnWidth: boolean;
}
declare function Body<RecordType>(props: BodyProps<RecordType>): React.JSX.Element;
declare namespace Body {
    var displayName: string;
}
declare const _default: typeof Body;
export default _default;
