import * as React from 'react';
import type { TabNavListProps } from '.';
export type TabNavListWrapperProps = Required<Omit<TabNavListProps, 'children' | 'className'>> & TabNavListProps;
declare const TabNavListWrapper: React.FC<TabNavListWrapperProps>;
export default TabNavListWrapper;
