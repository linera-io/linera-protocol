import * as React from 'react';
import type { DescriptionsItemType, InternalDescriptionsItemType } from '..';
import { type ScreenMap } from '../../_util/responsiveObserver';
export default function useItems(screens: ScreenMap, items?: DescriptionsItemType[], children?: React.ReactNode): InternalDescriptionsItemType[];
