import React from 'react';
import type { ListGridType } from '.';
export interface ListConsumerProps {
    grid?: ListGridType;
    itemLayout?: string;
}
export declare const ListContext: React.Context<ListConsumerProps>;
export declare const ListConsumer: React.Consumer<ListConsumerProps>;
