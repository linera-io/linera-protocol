"use strict";
"use client";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.VariantContext = exports.NoStyleItemContext = exports.NoFormStyle = exports.FormProvider = exports.FormItemPrefixContext = exports.FormItemInputContext = exports.FormContext = void 0;
var _react = _interopRequireWildcard(require("react"));
var React = _react;
var _rcFieldForm = require("rc-field-form");
var _omit = _interopRequireDefault(require("rc-util/lib/omit"));
const FormContext = exports.FormContext = /*#__PURE__*/React.createContext({
  labelAlign: 'right',
  vertical: false,
  itemRef: () => {}
});
const NoStyleItemContext = exports.NoStyleItemContext = /*#__PURE__*/React.createContext(null);
const FormProvider = props => {
  const providerProps = (0, _omit.default)(props, ['prefixCls']);
  return /*#__PURE__*/React.createElement(_rcFieldForm.FormProvider, Object.assign({}, providerProps));
};
exports.FormProvider = FormProvider;
const FormItemPrefixContext = exports.FormItemPrefixContext = /*#__PURE__*/React.createContext({
  prefixCls: ''
});
const FormItemInputContext = exports.FormItemInputContext = /*#__PURE__*/React.createContext({});
if (process.env.NODE_ENV !== 'production') {
  FormItemInputContext.displayName = 'FormItemInputContext';
}
const NoFormStyle = _ref => {
  let {
    children,
    status,
    override
  } = _ref;
  const formItemInputContext = (0, _react.useContext)(FormItemInputContext);
  const newFormItemInputContext = (0, _react.useMemo)(() => {
    const newContext = Object.assign({}, formItemInputContext);
    if (override) {
      delete newContext.isFormItemInput;
    }
    if (status) {
      delete newContext.status;
      delete newContext.hasFeedback;
      delete newContext.feedbackIcon;
    }
    return newContext;
  }, [status, override, formItemInputContext]);
  return /*#__PURE__*/React.createElement(FormItemInputContext.Provider, {
    value: newFormItemInputContext
  }, children);
};
exports.NoFormStyle = NoFormStyle;
const VariantContext = exports.VariantContext = /*#__PURE__*/(0, _react.createContext)(undefined);