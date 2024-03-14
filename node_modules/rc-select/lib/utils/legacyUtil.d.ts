import * as React from 'react';
import type { BaseOptionType, DefaultOptionType } from '../Select';
export declare function convertChildrenToData<OptionType extends BaseOptionType = DefaultOptionType>(nodes: React.ReactNode, optionOnly?: boolean): OptionType[];
