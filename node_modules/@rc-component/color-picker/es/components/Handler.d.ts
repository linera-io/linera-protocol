import type { FC } from 'react';
type HandlerSize = 'default' | 'small';
declare const Handler: FC<{
    size?: HandlerSize;
    color?: string;
    prefixCls?: string;
}>;
export default Handler;
