import * as React from 'react';
import type { AlignType } from '@rc-component/trigger';
import type { PickerMode } from 'rc-picker/lib/interface';
import type { SelectCommonPlacement } from '../_util/motion';
import type { DirectionType } from '../config-provider';
import type { PickerLocale, PickerProps } from './generatePicker';
export declare function getPlaceholder(locale: PickerLocale, picker?: PickerMode, customizePlaceholder?: string): string;
export declare function getRangePlaceholder(locale: PickerLocale, picker?: PickerMode, customizePlaceholder?: [string, string]): [string, string] | undefined;
export declare function transPlacement2DropdownAlign(direction: DirectionType, placement?: SelectCommonPlacement): AlignType;
export declare function useIcons(props: Pick<PickerProps, 'allowClear' | 'removeIcon'>, prefixCls: string): readonly [false | {
    clearIcon: React.ReactNode;
}, string | number | boolean | Iterable<React.ReactNode> | React.JSX.Element | ((props: any) => React.ReactNode) | null];
