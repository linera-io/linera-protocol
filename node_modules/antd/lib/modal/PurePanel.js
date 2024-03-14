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
var _rcDialog = require("rc-dialog");
var _PurePanel = require("../_util/PurePanel");
var _configProvider = require("../config-provider");
var _ConfirmDialog = require("./ConfirmDialog");
var _shared = require("./shared");
var _style = _interopRequireDefault(require("./style"));
var _useCSSVarCls = _interopRequireDefault(require("../config-provider/hooks/useCSSVarCls"));
var __rest = void 0 && (void 0).__rest || function (s, e) {
  var t = {};
  for (var p in s) if (Object.prototype.hasOwnProperty.call(s, p) && e.indexOf(p) < 0) t[p] = s[p];
  if (s != null && typeof Object.getOwnPropertySymbols === "function") for (var i = 0, p = Object.getOwnPropertySymbols(s); i < p.length; i++) {
    if (e.indexOf(p[i]) < 0 && Object.prototype.propertyIsEnumerable.call(s, p[i])) t[p[i]] = s[p[i]];
  }
  return t;
};
/* eslint-disable react/jsx-no-useless-fragment */

const PurePanel = props => {
  const {
      prefixCls: customizePrefixCls,
      className,
      closeIcon,
      closable,
      type,
      title,
      children,
      footer
    } = props,
    restProps = __rest(props, ["prefixCls", "className", "closeIcon", "closable", "type", "title", "children", "footer"]);
  const {
    getPrefixCls
  } = React.useContext(_configProvider.ConfigContext);
  const rootPrefixCls = getPrefixCls();
  const prefixCls = customizePrefixCls || getPrefixCls('modal');
  const rootCls = (0, _useCSSVarCls.default)(rootPrefixCls);
  const [wrapCSSVar, hashId, cssVarCls] = (0, _style.default)(prefixCls, rootCls);
  const confirmPrefixCls = `${prefixCls}-confirm`;
  // Choose target props by confirm mark
  let additionalProps = {};
  if (type) {
    additionalProps = {
      closable: closable !== null && closable !== void 0 ? closable : false,
      title: '',
      footer: '',
      children: ( /*#__PURE__*/React.createElement(_ConfirmDialog.ConfirmContent, Object.assign({}, props, {
        prefixCls: prefixCls,
        confirmPrefixCls: confirmPrefixCls,
        rootPrefixCls: rootPrefixCls,
        content: children
      })))
    };
  } else {
    additionalProps = {
      closable: closable !== null && closable !== void 0 ? closable : true,
      title,
      footer: footer !== null && /*#__PURE__*/React.createElement(_shared.Footer, Object.assign({}, props)),
      children
    };
  }
  return wrapCSSVar( /*#__PURE__*/React.createElement(_rcDialog.Panel, Object.assign({
    prefixCls: prefixCls,
    className: (0, _classnames.default)(hashId, `${prefixCls}-pure-panel`, type && confirmPrefixCls, type && `${confirmPrefixCls}-${type}`, className, cssVarCls, rootCls)
  }, restProps, {
    closeIcon: (0, _shared.renderCloseIcon)(prefixCls, closeIcon),
    closable: closable
  }, additionalProps)));
};
var _default = exports.default = (0, _PurePanel.withPureRenderTheme)(PurePanel);