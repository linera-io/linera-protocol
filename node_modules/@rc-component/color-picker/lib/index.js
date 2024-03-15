"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
var _exportNames = {
  Color: true,
  ColorBlock: true
};
Object.defineProperty(exports, "Color", {
  enumerable: true,
  get: function get() {
    return _color.Color;
  }
});
Object.defineProperty(exports, "ColorBlock", {
  enumerable: true,
  get: function get() {
    return _ColorBlock.default;
  }
});
exports.default = void 0;
var _ColorPicker = _interopRequireDefault(require("./ColorPicker"));
var _color = require("./color");
var _ColorBlock = _interopRequireDefault(require("./components/ColorBlock"));
var _interface = require("./interface");
Object.keys(_interface).forEach(function (key) {
  if (key === "default" || key === "__esModule") return;
  if (Object.prototype.hasOwnProperty.call(_exportNames, key)) return;
  if (key in exports && exports[key] === _interface[key]) return;
  Object.defineProperty(exports, key, {
    enumerable: true,
    get: function get() {
      return _interface[key];
    }
  });
});
var _default = exports.default = _ColorPicker.default;