"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.lintWarning = lintWarning;
var _warning = _interopRequireDefault(require("rc-util/lib/warning"));
function lintWarning(message, info) {
  var path = info.path,
    parentSelectors = info.parentSelectors;
  (0, _warning.default)(false, "[Ant Design CSS-in-JS] ".concat(path ? "Error in ".concat(path, ": ") : '').concat(message).concat(parentSelectors.length ? " Selector: ".concat(parentSelectors.join(' | ')) : ''));
}