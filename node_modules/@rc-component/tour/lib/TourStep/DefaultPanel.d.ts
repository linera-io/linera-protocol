import * as React from 'react';
import type { TourStepProps } from '../interface';
export type DefaultPanelProps = Exclude<TourStepProps, "closable"> & {
    closable: Exclude<TourStepProps["closable"], boolean>;
};
export default function DefaultPanel(props: DefaultPanelProps): React.JSX.Element;
