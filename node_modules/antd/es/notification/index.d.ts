import React from 'react';
import type { ArgsProps, GlobalConfigProps } from './interface';
import PurePanel from './PurePanel';
import useNotification from './useNotification';
export type { ArgsProps };
interface BaseMethods {
    open: (config: ArgsProps) => void;
    destroy: (key?: React.Key) => void;
    config: (config: GlobalConfigProps) => void;
    useNotification: typeof useNotification;
    /** @private Internal Component. Do not use in your production. */
    _InternalPanelDoNotUseOrYouWillBeFired: typeof PurePanel;
}
type StaticFn = (config: ArgsProps) => void;
interface NoticeMethods {
    success: StaticFn;
    info: StaticFn;
    warning: StaticFn;
    error: StaticFn;
}
declare const staticMethods: NoticeMethods & BaseMethods;
export default staticMethods;
