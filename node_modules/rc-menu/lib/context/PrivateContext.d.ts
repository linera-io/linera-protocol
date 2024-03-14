import * as React from 'react';
import type { MenuProps } from '../Menu';
export interface PrivateContextProps {
    _internalRenderMenuItem?: MenuProps['_internalRenderMenuItem'];
    _internalRenderSubMenuItem?: MenuProps['_internalRenderSubMenuItem'];
}
declare const PrivateContext: React.Context<PrivateContextProps>;
export default PrivateContext;
