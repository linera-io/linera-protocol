/**
 * BaseSelect provide some parsed data into context.
 * You can use this hooks to get them.
 */
import * as React from 'react';
import type { BaseSelectProps } from '../BaseSelect';
export interface BaseSelectContextProps extends BaseSelectProps {
    triggerOpen: boolean;
    multiple: boolean;
    toggleOpen: (open?: boolean) => void;
}
export declare const BaseSelectContext: React.Context<BaseSelectContextProps>;
export default function useBaseProps(): BaseSelectContextProps;
