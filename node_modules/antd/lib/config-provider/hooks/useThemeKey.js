"use strict";

var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var React = _interopRequireWildcard(require("react"));
const fullClone = Object.assign({}, React);
const {
  useId
} = fullClone;
const useEmptyId = () => '';
const useThemeKey = typeof useId === 'undefined' ? useEmptyId : useId;
var _default = exports.default = useThemeKey;