"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.geekblue = exports.cyan = exports.blue = void 0;
Object.defineProperty(exports, "generate", {
  enumerable: true,
  get: function get() {
    return _generate.default;
  }
});
exports.yellow = exports.volcano = exports.red = exports.purple = exports.presetPrimaryColors = exports.presetPalettes = exports.presetDarkPalettes = exports.orange = exports.magenta = exports.lime = exports.grey = exports.green = exports.gray = exports.gold = void 0;
var _generate = _interopRequireDefault(require("./generate"));
function _interopRequireDefault(obj) { return obj && obj.__esModule ? obj : { default: obj }; }
var presetPrimaryColors = exports.presetPrimaryColors = {
  red: '#F5222D',
  volcano: '#FA541C',
  orange: '#FA8C16',
  gold: '#FAAD14',
  yellow: '#FADB14',
  lime: '#A0D911',
  green: '#52C41A',
  cyan: '#13C2C2',
  blue: '#1677FF',
  geekblue: '#2F54EB',
  purple: '#722ED1',
  magenta: '#EB2F96',
  grey: '#666666'
};
var presetPalettes = exports.presetPalettes = {};
var presetDarkPalettes = exports.presetDarkPalettes = {};
Object.keys(presetPrimaryColors).forEach(function (key) {
  presetPalettes[key] = (0, _generate.default)(presetPrimaryColors[key]);
  presetPalettes[key].primary = presetPalettes[key][5];

  // dark presetPalettes
  presetDarkPalettes[key] = (0, _generate.default)(presetPrimaryColors[key], {
    theme: 'dark',
    backgroundColor: '#141414'
  });
  presetDarkPalettes[key].primary = presetDarkPalettes[key][5];
});
var red = exports.red = presetPalettes.red;
var volcano = exports.volcano = presetPalettes.volcano;
var gold = exports.gold = presetPalettes.gold;
var orange = exports.orange = presetPalettes.orange;
var yellow = exports.yellow = presetPalettes.yellow;
var lime = exports.lime = presetPalettes.lime;
var green = exports.green = presetPalettes.green;
var cyan = exports.cyan = presetPalettes.cyan;
var blue = exports.blue = presetPalettes.blue;
var geekblue = exports.geekblue = presetPalettes.geekblue;
var purple = exports.purple = presetPalettes.purple;
var magenta = exports.magenta = presetPalettes.magenta;
var grey = exports.grey = presetPalettes.grey;
var gray = exports.gray = presetPalettes.grey;