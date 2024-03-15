"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.isValidateOpenKey = isValidateOpenKey;
var _KeyCode = _interopRequireDefault(require("rc-util/lib/KeyCode"));
/** keyCode Judgment function */
function isValidateOpenKey(currentKeyCode) {
  return ![
  // System function button
  _KeyCode.default.ESC, _KeyCode.default.SHIFT, _KeyCode.default.BACKSPACE, _KeyCode.default.TAB, _KeyCode.default.WIN_KEY, _KeyCode.default.ALT, _KeyCode.default.META, _KeyCode.default.WIN_KEY_RIGHT, _KeyCode.default.CTRL, _KeyCode.default.SEMICOLON, _KeyCode.default.EQUALS, _KeyCode.default.CAPS_LOCK, _KeyCode.default.CONTEXT_MENU,
  // F1-F12
  _KeyCode.default.F1, _KeyCode.default.F2, _KeyCode.default.F3, _KeyCode.default.F4, _KeyCode.default.F5, _KeyCode.default.F6, _KeyCode.default.F7, _KeyCode.default.F8, _KeyCode.default.F9, _KeyCode.default.F10, _KeyCode.default.F11, _KeyCode.default.F12].includes(currentKeyCode);
}