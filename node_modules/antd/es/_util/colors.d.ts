import type { PresetColorKey } from '../theme/interface';
type InverseColor = `${PresetColorKey}-inverse`;
export declare const PresetStatusColorTypes: readonly ["success", "processing", "error", "default", "warning"];
export type PresetColorType = PresetColorKey | InverseColor;
export type PresetStatusColorType = typeof PresetStatusColorTypes[number];
/**
 * determine if the color keyword belongs to the `Ant Design` {@link PresetColors}.
 * @param color color to be judged
 * @param includeInverse whether to include reversed colors
 */
export declare function isPresetColor(color?: any, includeInverse?: boolean): boolean;
export declare function isPresetStatusColor(color?: any): color is PresetStatusColorType;
export {};
