"use strict";

var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard").default;
var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _slicedToArray2 = _interopRequireDefault(require("@babel/runtime/helpers/slicedToArray"));
var React = _interopRequireWildcard(require("react"));
var _index = require("./index");
var MockPortal = function MockPortal(_ref) {
  var open = _ref.open,
    autoDestroy = _ref.autoDestroy,
    children = _ref.children,
    getContainer = _ref.getContainer;
  var _React$useState = React.useState(open),
    _React$useState2 = (0, _slicedToArray2.default)(_React$useState, 2),
    visible = _React$useState2[0],
    setVisible = _React$useState2[1];
  React.useEffect(function () {
    getContainer === null || getContainer === void 0 || getContainer();
  });
  React.useEffect(function () {
    if (open) {
      setVisible(true);
    } else if (autoDestroy) {
      setVisible(false);
    }
  }, [open, autoDestroy]);
  return visible ? children : null;
};
var _default = exports.default = (0, _index.generateTrigger)(MockPortal);