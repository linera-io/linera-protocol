import * as React from 'react';
import type { ConfigOptions, MessageInstance } from './interface';
type HolderProps = ConfigOptions & {
    onAllRemoved?: VoidFunction;
};
export declare function useInternalMessage(messageConfig?: HolderProps): readonly [MessageInstance, React.ReactElement];
export default function useMessage(messageConfig?: ConfigOptions): readonly [MessageInstance, React.ReactElement<any, string | React.JSXElementConstructor<any>>];
export {};
