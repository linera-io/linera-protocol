"use strict";
"use client";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var React = _interopRequireWildcard(require("react"));
var _classnames = _interopRequireDefault(require("classnames"));
var _toArray = _interopRequireDefault(require("rc-util/lib/Children/toArray"));
var _omit = _interopRequireDefault(require("rc-util/lib/omit"));
var _useZIndex = require("../_util/hooks/useZIndex");
var _PurePanel = _interopRequireDefault(require("../_util/PurePanel"));
var _warning = require("../_util/warning");
var _configProvider = require("../config-provider");
var _select = _interopRequireDefault(require("../select"));
const {
  Option
} = _select.default;
function isSelectOptionOrSelectOptGroup(child) {
  return child && child.type && (child.type.isSelectOption || child.type.isSelectOptGroup);
}
const AutoComplete = (props, ref) => {
  var _a;
  const {
    prefixCls: customizePrefixCls,
    className,
    popupClassName,
    dropdownClassName,
    children,
    dataSource
  } = props;
  const childNodes = (0, _toArray.default)(children);
  // ============================= Input =============================
  let customizeInput;
  if (childNodes.length === 1 && /*#__PURE__*/React.isValidElement(childNodes[0]) && !isSelectOptionOrSelectOptGroup(childNodes[0])) {
    [customizeInput] = childNodes;
  }
  const getInputElement = customizeInput ? () => customizeInput : undefined;
  // ============================ Options ============================
  let optionChildren;
  // [Legacy] convert `children` or `dataSource` into option children
  if (childNodes.length && isSelectOptionOrSelectOptGroup(childNodes[0])) {
    optionChildren = children;
  } else {
    optionChildren = dataSource ? dataSource.map(item => {
      if ( /*#__PURE__*/React.isValidElement(item)) {
        return item;
      }
      switch (typeof item) {
        case 'string':
          return /*#__PURE__*/React.createElement(Option, {
            key: item,
            value: item
          }, item);
        case 'object':
          {
            const {
              value: optionValue
            } = item;
            return /*#__PURE__*/React.createElement(Option, {
              key: optionValue,
              value: optionValue
            }, item.text);
          }
        default:
          return undefined;
      }
    }) : [];
  }
  if (process.env.NODE_ENV !== 'production') {
    const warning = (0, _warning.devUseWarning)('AutoComplete');
    warning.deprecated(!('dataSource' in props), 'dataSource', 'options');
    process.env.NODE_ENV !== "production" ? warning(!customizeInput || !('size' in props), 'usage', 'You need to control style self instead of setting `size` when using customize input.') : void 0;
    warning.deprecated(!dropdownClassName, 'dropdownClassName', 'popupClassName');
  }
  const {
    getPrefixCls
  } = React.useContext(_configProvider.ConfigContext);
  const prefixCls = getPrefixCls('select', customizePrefixCls);
  // ============================ zIndex ============================
  const [zIndex] = (0, _useZIndex.useZIndex)('SelectLike', (_a = props.dropdownStyle) === null || _a === void 0 ? void 0 : _a.zIndex);
  return /*#__PURE__*/React.createElement(_select.default, Object.assign({
    ref: ref,
    suffixIcon: null
  }, (0, _omit.default)(props, ['dataSource', 'dropdownClassName']), {
    prefixCls: prefixCls,
    popupClassName: popupClassName || dropdownClassName,
    dropdownStyle: Object.assign(Object.assign({}, props.dropdownStyle), {
      zIndex
    }),
    className: (0, _classnames.default)(`${prefixCls}-auto-complete`, className),
    mode: _select.default.SECRET_COMBOBOX_MODE_DO_NOT_USE,
    // Internal api
    getInputElement
  }), optionChildren);
};
const RefAutoComplete = /*#__PURE__*/React.forwardRef(AutoComplete);
// We don't care debug panel
/* istanbul ignore next */
const PurePanel = (0, _PurePanel.default)(RefAutoComplete);
RefAutoComplete.Option = Option;
RefAutoComplete._InternalPanelDoNotUseOrYouWillBeFired = PurePanel;
if (process.env.NODE_ENV !== 'production') {
  RefAutoComplete.displayName = 'AutoComplete';
}
var _default = exports.default = RefAutoComplete;