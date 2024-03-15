import React from 'react';
import type { TourProps } from './interface';
import PurePanel from './PurePanel';
declare const Tour: React.FC<TourProps> & {
    _InternalPanelDoNotUseOrYouWillBeFired: typeof PurePanel;
};
export default Tour;
