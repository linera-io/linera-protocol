import React from 'react';
interface EmptyProps {
    componentName?: string;
}
declare const DefaultRenderEmpty: React.FC<EmptyProps>;
export type RenderEmptyHandler = (componentName?: string) => React.ReactNode;
export default DefaultRenderEmpty;
