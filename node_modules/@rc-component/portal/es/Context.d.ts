import * as React from 'react';
export declare type QueueCreate = (appendFunc: VoidFunction) => void;
declare const OrderContext: React.Context<QueueCreate>;
export default OrderContext;
