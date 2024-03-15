import * as React from 'react';
import type { AnimatedConfig, TabPosition } from '../interface';
export interface TabPanelListProps {
    activeKey: string;
    id: string;
    animated?: AnimatedConfig;
    tabPosition?: TabPosition;
    destroyInactiveTabPane?: boolean;
}
declare const TabPanelList: React.FC<TabPanelListProps>;
export default TabPanelList;
