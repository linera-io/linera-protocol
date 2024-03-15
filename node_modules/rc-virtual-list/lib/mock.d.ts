import * as React from 'react';
import type { ListProps, ListRef } from './List';
declare const List: <Item = any>(props: ListProps<Item> & {
    children?: React.ReactNode;
} & {
    ref?: React.Ref<ListRef>;
}) => React.ReactElement;
export default List;
