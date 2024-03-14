import * as React from 'react';
import type { OptionProps } from './Option';
export interface MentionsContextProps {
    notFoundContent: React.ReactNode;
    activeIndex: number;
    setActiveIndex: (index: number) => void;
    selectOption: (option: OptionProps) => void;
    onFocus: React.FocusEventHandler<HTMLElement>;
    onBlur: React.FocusEventHandler<HTMLElement>;
}
declare const MentionsContext: React.Context<MentionsContextProps>;
export default MentionsContext;
