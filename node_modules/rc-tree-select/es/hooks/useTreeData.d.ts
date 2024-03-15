import * as React from 'react';
import type { DataNode, SimpleModeConfig } from '../interface';
import type { DefaultOptionType } from '../TreeSelect';
/**
 * Convert `treeData` or `children` into formatted `treeData`.
 * Will not re-calculate if `treeData` or `children` not change.
 */
export default function useTreeData(treeData: DataNode[], children: React.ReactNode, simpleMode: boolean | SimpleModeConfig): DefaultOptionType[];
