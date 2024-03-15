"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.canUseDocElement = void 0;
Object.defineProperty(exports, "isStyleSupport", {
  enumerable: true,
  get: function () {
    return _styleChecker.isStyleSupport;
  }
});
var _canUseDom = _interopRequireDefault(require("rc-util/lib/Dom/canUseDom"));
var _styleChecker = require("rc-util/lib/Dom/styleChecker");
const canUseDocElement = () => (0, _canUseDom.default)() && window.document.documentElement;
exports.canUseDocElement = canUseDocElement;