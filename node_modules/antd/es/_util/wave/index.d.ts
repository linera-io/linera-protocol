import React from 'react';
export interface WaveProps {
    disabled?: boolean;
    children?: React.ReactNode;
    component?: string;
}
declare const Wave: React.FC<WaveProps>;
export default Wave;
