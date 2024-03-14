import React from 'react';
import type { ConfigOptions as MessageConfig, MessageInstance } from '../message/interface';
import type { HookAPI as ModalHookAPI } from '../modal/useModal';
import type { NotificationConfig, NotificationInstance } from '../notification/interface';
export interface AppConfig {
    message?: MessageConfig;
    notification?: NotificationConfig;
}
export declare const AppConfigContext: React.Context<AppConfig>;
export interface useAppProps {
    message: MessageInstance;
    notification: NotificationInstance;
    modal: ModalHookAPI;
}
declare const AppContext: React.Context<useAppProps>;
export default AppContext;
