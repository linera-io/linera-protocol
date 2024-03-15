import _toConsumableArray from "@babel/runtime/helpers/esm/toConsumableArray";
import { PresetColors } from '../theme/interface';
const inverseColors = PresetColors.map(color => `${color}-inverse`);
export const PresetStatusColorTypes = ['success', 'processing', 'error', 'default', 'warning'];
/**
 * determine if the color keyword belongs to the `Ant Design` {@link PresetColors}.
 * @param color color to be judged
 * @param includeInverse whether to include reversed colors
 */
export function isPresetColor(color) {
  let includeInverse = arguments.length > 1 && arguments[1] !== undefined ? arguments[1] : true;
  if (includeInverse) {
    return [].concat(_toConsumableArray(inverseColors), _toConsumableArray(PresetColors)).includes(color);
  }
  return PresetColors.includes(color);
}
export function isPresetStatusColor(color) {
  return PresetStatusColorTypes.includes(color);
}