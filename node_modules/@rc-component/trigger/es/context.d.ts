import * as React from 'react';
export interface TriggerContextProps {
    registerSubPopup: (id: string, node: HTMLElement) => void;
}
declare const TriggerContext: React.Context<TriggerContextProps>;
export default TriggerContext;
