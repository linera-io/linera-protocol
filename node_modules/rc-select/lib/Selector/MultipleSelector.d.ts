import * as React from 'react';
import type { InnerSelectorProps } from '.';
import type { DisplayValueType, RenderNode, CustomTagProps } from '../BaseSelect';
interface SelectorProps extends InnerSelectorProps {
    removeIcon?: RenderNode;
    maxTagCount?: number | 'responsive';
    maxTagTextLength?: number;
    maxTagPlaceholder?: React.ReactNode | ((omittedValues: DisplayValueType[]) => React.ReactNode);
    tokenSeparators?: string[];
    tagRender?: (props: CustomTagProps) => React.ReactElement;
    onToggleOpen: (open?: boolean) => void;
    choiceTransitionName?: string;
    onRemove: (value: DisplayValueType) => void;
}
declare const SelectSelector: React.FC<SelectorProps>;
export default SelectSelector;
