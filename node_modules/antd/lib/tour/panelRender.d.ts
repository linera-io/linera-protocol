import React from 'react';
import type { TourStepProps } from './interface';
interface TourPanelProps {
    stepProps: Omit<TourStepProps, 'closable'> & {
        closable?: Exclude<TourStepProps['closable'], boolean>;
    };
    current: number;
    type: TourStepProps['type'];
    indicatorsRender?: TourStepProps['indicatorsRender'];
}
declare const TourPanel: React.FC<TourPanelProps>;
export default TourPanel;
