"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var React = _interopRequireWildcard(require("react"));
var _DefaultPanel = _interopRequireDefault(require("./DefaultPanel"));
var TourStep = function TourStep(props) {
  var current = props.current,
    renderPanel = props.renderPanel;
  return /*#__PURE__*/React.createElement(React.Fragment, null, typeof renderPanel === 'function' ? renderPanel(props, current) : /*#__PURE__*/React.createElement(_DefaultPanel.default, props));
};
var _default = exports.default = TourStep;