"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.PanelPresetsProvider = exports.PanelPresetsContext = exports.PanelPickerProvider = exports.PanelPickerContext = void 0;
var _react = _interopRequireDefault(require("react"));
const PanelPickerContext = exports.PanelPickerContext = /*#__PURE__*/_react.default.createContext({});
const PanelPresetsContext = exports.PanelPresetsContext = /*#__PURE__*/_react.default.createContext({});
const {
  Provider: PanelPickerProvider
} = PanelPickerContext;
exports.PanelPickerProvider = PanelPickerProvider;
const {
  Provider: PanelPresetsProvider
} = PanelPresetsContext;
exports.PanelPresetsProvider = PanelPresetsProvider;