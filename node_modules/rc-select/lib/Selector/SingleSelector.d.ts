import * as React from 'react';
import type { InnerSelectorProps } from '.';
interface SelectorProps extends InnerSelectorProps {
    inputElement: React.ReactElement;
    activeValue: string;
}
declare const SingleSelector: React.FC<SelectorProps>;
export default SingleSelector;
