import type { DisabledTimes, PickerMode } from '../interface';
export interface WarningProps extends DisabledTimes {
    picker?: PickerMode;
}
export declare function legacyPropsWarning(props: WarningProps): void;
