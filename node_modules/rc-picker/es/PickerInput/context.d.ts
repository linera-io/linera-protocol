import * as React from 'react';
import type { GenerateConfig } from '../generate';
import type { Components, Locale } from '../interface';
export interface PickerContextProps<DateType = any> {
    prefixCls: string;
    locale: Locale;
    generateConfig: GenerateConfig<DateType>;
    /** Customize button component */
    button?: Components['button'];
    input?: Components['input'];
}
declare const PickerContext: React.Context<PickerContextProps<any>>;
export default PickerContext;
