import * as React from 'react';
import type { AriaValueFormat, Direction, SliderClassNames, SliderStyles } from './interface';
export interface SliderContextProps {
    min: number;
    max: number;
    includedStart: number;
    includedEnd: number;
    direction: Direction;
    disabled?: boolean;
    keyboard?: boolean;
    included?: boolean;
    step: number | null;
    range?: boolean;
    tabIndex: number | number[];
    ariaLabelForHandle?: string | string[];
    ariaLabelledByForHandle?: string | string[];
    ariaValueTextFormatterForHandle?: AriaValueFormat | AriaValueFormat[];
    classNames: SliderClassNames;
    styles: SliderStyles;
}
declare const SliderContext: React.Context<SliderContextProps>;
export default SliderContext;
