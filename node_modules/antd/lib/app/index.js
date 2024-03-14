"use strict";
"use client";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _react = _interopRequireWildcard(require("react"));
var _classnames = _interopRequireDefault(require("classnames"));
var _configProvider = require("../config-provider");
var _useMessage = _interopRequireDefault(require("../message/useMessage"));
var _useModal = _interopRequireDefault(require("../modal/useModal"));
var _useNotification = _interopRequireDefault(require("../notification/useNotification"));
var _context = _interopRequireWildcard(require("./context"));
var _style = _interopRequireDefault(require("./style"));
const useApp = () => _react.default.useContext(_context.default);
const App = props => {
  const {
    prefixCls: customizePrefixCls,
    children,
    className,
    rootClassName,
    message,
    notification,
    style,
    component = 'div'
  } = props;
  const {
    getPrefixCls
  } = (0, _react.useContext)(_configProvider.ConfigContext);
  const prefixCls = getPrefixCls('app', customizePrefixCls);
  const [wrapCSSVar, hashId, cssVarCls] = (0, _style.default)(prefixCls);
  const customClassName = (0, _classnames.default)(hashId, prefixCls, className, rootClassName, cssVarCls);
  const appConfig = (0, _react.useContext)(_context.AppConfigContext);
  const mergedAppConfig = _react.default.useMemo(() => ({
    message: Object.assign(Object.assign({}, appConfig.message), message),
    notification: Object.assign(Object.assign({}, appConfig.notification), notification)
  }), [message, notification, appConfig.message, appConfig.notification]);
  const [messageApi, messageContextHolder] = (0, _useMessage.default)(mergedAppConfig.message);
  const [notificationApi, notificationContextHolder] = (0, _useNotification.default)(mergedAppConfig.notification);
  const [ModalApi, ModalContextHolder] = (0, _useModal.default)();
  const memoizedContextValue = _react.default.useMemo(() => ({
    message: messageApi,
    notification: notificationApi,
    modal: ModalApi
  }), [messageApi, notificationApi, ModalApi]);
  // ============================ Render ============================
  const Component = component === false ? _react.default.Fragment : component;
  const rootProps = {
    className: customClassName,
    style
  };
  return wrapCSSVar( /*#__PURE__*/_react.default.createElement(_context.default.Provider, {
    value: memoizedContextValue
  }, /*#__PURE__*/_react.default.createElement(_context.AppConfigContext.Provider, {
    value: mergedAppConfig
  }, /*#__PURE__*/_react.default.createElement(Component, Object.assign({}, component === false ? undefined : rootProps), ModalContextHolder, messageContextHolder, notificationContextHolder, children))));
};
if (process.env.NODE_ENV !== 'production') {
  App.displayName = 'App';
}
App.useApp = useApp;
var _default = exports.default = App;