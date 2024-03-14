"use strict";
"use client";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = useMessage;
exports.useInternalMessage = useInternalMessage;
var React = _interopRequireWildcard(require("react"));
var _CloseOutlined = _interopRequireDefault(require("@ant-design/icons/CloseOutlined"));
var _classnames = _interopRequireDefault(require("classnames"));
var _rcNotification = require("rc-notification");
var _warning = require("../_util/warning");
var _configProvider = require("../config-provider");
var _useCSSVarCls = _interopRequireDefault(require("../config-provider/hooks/useCSSVarCls"));
var _PurePanel = require("./PurePanel");
var _style = _interopRequireDefault(require("./style"));
var _util = require("./util");
var __rest = void 0 && (void 0).__rest || function (s, e) {
  var t = {};
  for (var p in s) if (Object.prototype.hasOwnProperty.call(s, p) && e.indexOf(p) < 0) t[p] = s[p];
  if (s != null && typeof Object.getOwnPropertySymbols === "function") for (var i = 0, p = Object.getOwnPropertySymbols(s); i < p.length; i++) {
    if (e.indexOf(p[i]) < 0 && Object.prototype.propertyIsEnumerable.call(s, p[i])) t[p[i]] = s[p[i]];
  }
  return t;
};
const DEFAULT_OFFSET = 8;
const DEFAULT_DURATION = 3;
const Wrapper = _ref => {
  let {
    children,
    prefixCls
  } = _ref;
  const rootCls = (0, _useCSSVarCls.default)(prefixCls);
  const [wrapCSSVar, hashId, cssVarCls] = (0, _style.default)(prefixCls, rootCls);
  return wrapCSSVar( /*#__PURE__*/React.createElement(_rcNotification.NotificationProvider, {
    classNames: {
      list: (0, _classnames.default)(hashId, cssVarCls, rootCls)
    }
  }, children));
};
const renderNotifications = (node, _ref2) => {
  let {
    prefixCls,
    key
  } = _ref2;
  return /*#__PURE__*/React.createElement(Wrapper, {
    prefixCls: prefixCls,
    key: key
  }, node);
};
const Holder = /*#__PURE__*/React.forwardRef((props, ref) => {
  const {
    top,
    prefixCls: staticPrefixCls,
    getContainer: staticGetContainer,
    maxCount,
    duration = DEFAULT_DURATION,
    rtl,
    transitionName,
    onAllRemoved
  } = props;
  const {
    getPrefixCls,
    getPopupContainer,
    message,
    direction
  } = React.useContext(_configProvider.ConfigContext);
  const prefixCls = staticPrefixCls || getPrefixCls('message');
  // =============================== Style ===============================
  const getStyle = () => ({
    left: '50%',
    transform: 'translateX(-50%)',
    top: top !== null && top !== void 0 ? top : DEFAULT_OFFSET
  });
  const getClassName = () => (0, _classnames.default)({
    [`${prefixCls}-rtl`]: rtl !== null && rtl !== void 0 ? rtl : direction === 'rtl'
  });
  // ============================== Motion ===============================
  const getNotificationMotion = () => (0, _util.getMotion)(prefixCls, transitionName);
  // ============================ Close Icon =============================
  const mergedCloseIcon = /*#__PURE__*/React.createElement("span", {
    className: `${prefixCls}-close-x`
  }, /*#__PURE__*/React.createElement(_CloseOutlined.default, {
    className: `${prefixCls}-close-icon`
  }));
  // ============================== Origin ===============================
  const [api, holder] = (0, _rcNotification.useNotification)({
    prefixCls,
    style: getStyle,
    className: getClassName,
    motion: getNotificationMotion,
    closable: false,
    closeIcon: mergedCloseIcon,
    duration,
    getContainer: () => (staticGetContainer === null || staticGetContainer === void 0 ? void 0 : staticGetContainer()) || (getPopupContainer === null || getPopupContainer === void 0 ? void 0 : getPopupContainer()) || document.body,
    maxCount,
    onAllRemoved,
    renderNotifications
  });
  // ================================ Ref ================================
  React.useImperativeHandle(ref, () => Object.assign(Object.assign({}, api), {
    prefixCls,
    message
  }));
  return holder;
});
// ==============================================================================
// ==                                   Hook                                   ==
// ==============================================================================
let keyIndex = 0;
function useInternalMessage(messageConfig) {
  const holderRef = React.useRef(null);
  const warning = (0, _warning.devUseWarning)('Message');
  // ================================ API ================================
  const wrapAPI = React.useMemo(() => {
    // Wrap with notification content
    // >>> close
    const close = key => {
      var _a;
      (_a = holderRef.current) === null || _a === void 0 ? void 0 : _a.close(key);
    };
    // >>> Open
    const open = config => {
      if (!holderRef.current) {
        process.env.NODE_ENV !== "production" ? warning(false, 'usage', 'You are calling notice in render which will break in React 18 concurrent mode. Please trigger in effect instead.') : void 0;
        const fakeResult = () => {};
        fakeResult.then = () => {};
        return fakeResult;
      }
      const {
        open: originOpen,
        prefixCls,
        message
      } = holderRef.current;
      const noticePrefixCls = `${prefixCls}-notice`;
      const {
          content,
          icon,
          type,
          key,
          className,
          style,
          onClose
        } = config,
        restConfig = __rest(config, ["content", "icon", "type", "key", "className", "style", "onClose"]);
      let mergedKey = key;
      if (mergedKey === undefined || mergedKey === null) {
        keyIndex += 1;
        mergedKey = `antd-message-${keyIndex}`;
      }
      return (0, _util.wrapPromiseFn)(resolve => {
        originOpen(Object.assign(Object.assign({}, restConfig), {
          key: mergedKey,
          content: ( /*#__PURE__*/React.createElement(_PurePanel.PureContent, {
            prefixCls: prefixCls,
            type: type,
            icon: icon
          }, content)),
          placement: 'top',
          className: (0, _classnames.default)(type && `${noticePrefixCls}-${type}`, className, message === null || message === void 0 ? void 0 : message.className),
          style: Object.assign(Object.assign({}, message === null || message === void 0 ? void 0 : message.style), style),
          onClose: () => {
            onClose === null || onClose === void 0 ? void 0 : onClose();
            resolve();
          }
        }));
        // Return close function
        return () => {
          close(mergedKey);
        };
      });
    };
    // >>> destroy
    const destroy = key => {
      var _a;
      if (key !== undefined) {
        close(key);
      } else {
        (_a = holderRef.current) === null || _a === void 0 ? void 0 : _a.destroy();
      }
    };
    const clone = {
      open,
      destroy
    };
    const keys = ['info', 'success', 'warning', 'error', 'loading'];
    keys.forEach(type => {
      const typeOpen = (jointContent, duration, onClose) => {
        let config;
        if (jointContent && typeof jointContent === 'object' && 'content' in jointContent) {
          config = jointContent;
        } else {
          config = {
            content: jointContent
          };
        }
        // Params
        let mergedDuration;
        let mergedOnClose;
        if (typeof duration === 'function') {
          mergedOnClose = duration;
        } else {
          mergedDuration = duration;
          mergedOnClose = onClose;
        }
        const mergedConfig = Object.assign(Object.assign({
          onClose: mergedOnClose,
          duration: mergedDuration
        }, config), {
          type
        });
        return open(mergedConfig);
      };
      clone[type] = typeOpen;
    });
    return clone;
  }, []);
  // ============================== Return ===============================
  return [wrapAPI, /*#__PURE__*/React.createElement(Holder, Object.assign({
    key: "message-holder"
  }, messageConfig, {
    ref: holderRef
  }))];
}
function useMessage(messageConfig) {
  return useInternalMessage(messageConfig);
}