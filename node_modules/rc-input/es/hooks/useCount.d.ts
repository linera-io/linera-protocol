import type { InputProps } from '..';
import type { CountConfig, ShowCountFormatter } from '../interface';
type ForcedCountConfig = Omit<CountConfig, 'show'> & Pick<Required<CountConfig>, 'strategy'> & {
    show: boolean;
    showFormatter?: ShowCountFormatter;
};
/**
 * Cut `value` by the `count.max` prop.
 */
export declare function inCountRange(value: string, countConfig: ForcedCountConfig): boolean;
export default function useCount(count?: CountConfig, showCount?: InputProps['showCount']): ForcedCountConfig;
export {};
