import type { ScrollConfig } from 'rc-virtual-list/lib/List';
import * as React from 'react';
export type OptionListProps = Record<string, never>;
export interface RefOptionListProps {
    onKeyDown: React.KeyboardEventHandler;
    onKeyUp: React.KeyboardEventHandler;
    scrollTo?: (args: number | ScrollConfig) => void;
}
declare const RefOptionList: React.ForwardRefExoticComponent<React.RefAttributes<RefOptionListProps>>;
export default RefOptionList;
