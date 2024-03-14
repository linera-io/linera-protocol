import { type RenderEmptyHandler } from '../../config-provider';
export default function useBase(customizePrefixCls?: string, direction?: 'ltr' | 'rtl'): [
    prefixCls: string,
    cascaderPrefixCls: string,
    direction?: 'ltr' | 'rtl',
    renderEmpty?: RenderEmptyHandler
];
