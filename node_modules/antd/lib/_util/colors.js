"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.PresetStatusColorTypes = void 0;
exports.isPresetColor = isPresetColor;
exports.isPresetStatusColor = isPresetStatusColor;
var _toConsumableArray2 = _interopRequireDefault(require("@babel/runtime/helpers/toConsumableArray"));
var _interface = require("../theme/interface");
const inverseColors = _interface.PresetColors.map(color => `${color}-inverse`);
const PresetStatusColorTypes = exports.PresetStatusColorTypes = ['success', 'processing', 'error', 'default', 'warning'];
/**
 * determine if the color keyword belongs to the `Ant Design` {@link PresetColors}.
 * @param color color to be judged
 * @param includeInverse whether to include reversed colors
 */
function isPresetColor(color) {
  let includeInverse = arguments.length > 1 && arguments[1] !== undefined ? arguments[1] : true;
  if (includeInverse) {
    return [].concat((0, _toConsumableArray2.default)(inverseColors), (0, _toConsumableArray2.default)(_interface.PresetColors)).includes(color);
  }
  return _interface.PresetColors.includes(color);
}
function isPresetStatusColor(color) {
  return PresetStatusColorTypes.includes(color);
}