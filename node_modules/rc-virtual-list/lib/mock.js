"use strict";

var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard").default;
var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _objectSpread2 = _interopRequireDefault(require("@babel/runtime/helpers/objectSpread2"));
var React = _interopRequireWildcard(require("react"));
var _List = require("./List");
var List = /*#__PURE__*/React.forwardRef(function (props, ref) {
  return (0, _List.RawList)((0, _objectSpread2.default)((0, _objectSpread2.default)({}, props), {}, {
    virtual: false
  }), ref);
});
List.displayName = 'List';
var _default = exports.default = List;