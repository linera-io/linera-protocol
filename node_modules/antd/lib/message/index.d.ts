import React from 'react';
import type { ArgsProps, ConfigOptions, MessageType, TypeOpen } from './interface';
import PurePanel from './PurePanel';
import useMessage from './useMessage';
export type { ArgsProps };
declare function setMessageGlobalConfig(config: ConfigOptions): void;
interface BaseMethods {
    open: (config: ArgsProps) => MessageType;
    destroy: (key?: React.Key) => void;
    config: typeof setMessageGlobalConfig;
    useMessage: typeof useMessage;
    /** @private Internal Component. Do not use in your production. */
    _InternalPanelDoNotUseOrYouWillBeFired: typeof PurePanel;
}
interface MessageMethods {
    info: TypeOpen;
    success: TypeOpen;
    error: TypeOpen;
    warning: TypeOpen;
    loading: TypeOpen;
}
declare const staticMethods: MessageMethods & BaseMethods;
export default staticMethods;
