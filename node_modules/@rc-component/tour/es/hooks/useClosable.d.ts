import * as React from 'react';
import type { TourProps } from '../interface';
import type { TourStepInfo } from '../TourStep';
export declare function useClosable(stepClosable: TourStepInfo['closable'], stepCloseIcon: TourStepInfo['closeIcon'], closable: TourProps['closable'], closeIcon: TourProps['closeIcon']): {
    closeIcon?: React.ReactNode;
} & React.AriaAttributes;
