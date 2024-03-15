import * as React from 'react';
import type { InternalNamePath } from './interface';
export interface ListContextProps {
    getKey: (namePath: InternalNamePath) => [InternalNamePath[number], InternalNamePath];
}
declare const ListContext: React.Context<ListContextProps>;
export default ListContext;
