import type { ThemeConfig } from '../context';
export default function useTheme(theme?: ThemeConfig, parentTheme?: ThemeConfig, config?: {
    prefixCls?: string;
}): ThemeConfig | undefined;
