"use strict";
"use client";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _BackTop = _interopRequireDefault(require("./BackTop"));
var _FloatButton = _interopRequireDefault(require("./FloatButton"));
var _FloatButtonGroup = _interopRequireDefault(require("./FloatButtonGroup"));
var _PurePanel = _interopRequireDefault(require("./PurePanel"));
_FloatButton.default.BackTop = _BackTop.default;
_FloatButton.default.Group = _FloatButtonGroup.default;
_FloatButton.default._InternalPanelDoNotUseOrYouWillBeFired = _PurePanel.default;
var _default = exports.default = _FloatButton.default;