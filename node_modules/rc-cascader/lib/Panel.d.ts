import * as React from 'react';
import type { CascaderProps } from './Cascader';
export type PickType = 'value' | 'defaultValue' | 'changeOnSelect' | 'onChange' | 'options' | 'prefixCls' | 'checkable' | 'fieldNames' | 'showCheckedStrategy' | 'loadData' | 'expandTrigger' | 'expandIcon' | 'loadingIcon' | 'className' | 'style' | 'direction' | 'notFoundContent';
export type PanelProps = Pick<CascaderProps, PickType>;
export default function Panel(props: PanelProps): React.JSX.Element;
