import React from 'react';
export interface DescriptionsContextProps {
    labelStyle?: React.CSSProperties;
    contentStyle?: React.CSSProperties;
}
declare const DescriptionsContext: React.Context<DescriptionsContextProps>;
export default DescriptionsContext;
