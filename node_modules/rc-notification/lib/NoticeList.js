"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");
var _typeof = require("@babel/runtime/helpers/typeof");
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _extends2 = _interopRequireDefault(require("@babel/runtime/helpers/extends"));
var _toConsumableArray2 = _interopRequireDefault(require("@babel/runtime/helpers/toConsumableArray"));
var _objectSpread2 = _interopRequireDefault(require("@babel/runtime/helpers/objectSpread2"));
var _objectWithoutProperties2 = _interopRequireDefault(require("@babel/runtime/helpers/objectWithoutProperties"));
var _defineProperty2 = _interopRequireDefault(require("@babel/runtime/helpers/defineProperty"));
var _slicedToArray2 = _interopRequireDefault(require("@babel/runtime/helpers/slicedToArray"));
var _react = _interopRequireWildcard(require("react"));
var _classnames = _interopRequireDefault(require("classnames"));
var _rcMotion = require("rc-motion");
var _Notice = _interopRequireDefault(require("./Notice"));
var _NotificationProvider = require("./NotificationProvider");
var _useStack3 = _interopRequireDefault(require("./hooks/useStack"));
var _excluded = ["className", "style", "classNames", "styles"];
function _getRequireWildcardCache(e) { if ("function" != typeof WeakMap) return null; var r = new WeakMap(), t = new WeakMap(); return (_getRequireWildcardCache = function _getRequireWildcardCache(e) { return e ? t : r; })(e); }
function _interopRequireWildcard(e, r) { if (!r && e && e.__esModule) return e; if (null === e || "object" != _typeof(e) && "function" != typeof e) return { default: e }; var t = _getRequireWildcardCache(r); if (t && t.has(e)) return t.get(e); var n = { __proto__: null }, a = Object.defineProperty && Object.getOwnPropertyDescriptor; for (var u in e) if ("default" !== u && Object.prototype.hasOwnProperty.call(e, u)) { var i = a ? Object.getOwnPropertyDescriptor(e, u) : null; i && (i.get || i.set) ? Object.defineProperty(n, u, i) : n[u] = e[u]; } return n.default = e, t && t.set(e, n), n; }
var NoticeList = function NoticeList(props) {
  var _clsx;
  var configList = props.configList,
    placement = props.placement,
    prefixCls = props.prefixCls,
    className = props.className,
    style = props.style,
    motion = props.motion,
    onAllNoticeRemoved = props.onAllNoticeRemoved,
    onNoticeClose = props.onNoticeClose,
    stackConfig = props.stack;
  var _useContext = (0, _react.useContext)(_NotificationProvider.NotificationContext),
    ctxCls = _useContext.classNames;
  var dictRef = (0, _react.useRef)({});
  var _useState = (0, _react.useState)(null),
    _useState2 = (0, _slicedToArray2.default)(_useState, 2),
    latestNotice = _useState2[0],
    setLatestNotice = _useState2[1];
  var _useState3 = (0, _react.useState)([]),
    _useState4 = (0, _slicedToArray2.default)(_useState3, 2),
    hoverKeys = _useState4[0],
    setHoverKeys = _useState4[1];
  var keys = configList.map(function (config) {
    return {
      config: config,
      key: String(config.key)
    };
  });
  var _useStack = (0, _useStack3.default)(stackConfig),
    _useStack2 = (0, _slicedToArray2.default)(_useStack, 2),
    stack = _useStack2[0],
    _useStack2$ = _useStack2[1],
    offset = _useStack2$.offset,
    threshold = _useStack2$.threshold,
    gap = _useStack2$.gap;
  var expanded = stack && (hoverKeys.length > 0 || keys.length <= threshold);
  var placementMotion = typeof motion === 'function' ? motion(placement) : motion;

  // Clean hover key
  (0, _react.useEffect)(function () {
    if (stack && hoverKeys.length > 1) {
      setHoverKeys(function (prev) {
        return prev.filter(function (key) {
          return keys.some(function (_ref) {
            var dataKey = _ref.key;
            return key === dataKey;
          });
        });
      });
    }
  }, [hoverKeys, keys, stack]);

  // Force update latest notice
  (0, _react.useEffect)(function () {
    var _keys;
    if (stack && dictRef.current[(_keys = keys[keys.length - 1]) === null || _keys === void 0 ? void 0 : _keys.key]) {
      var _keys2;
      setLatestNotice(dictRef.current[(_keys2 = keys[keys.length - 1]) === null || _keys2 === void 0 ? void 0 : _keys2.key]);
    }
  }, [keys, stack]);
  return /*#__PURE__*/_react.default.createElement(_rcMotion.CSSMotionList, (0, _extends2.default)({
    key: placement,
    className: (0, _classnames.default)(prefixCls, "".concat(prefixCls, "-").concat(placement), ctxCls === null || ctxCls === void 0 ? void 0 : ctxCls.list, className, (_clsx = {}, (0, _defineProperty2.default)(_clsx, "".concat(prefixCls, "-stack"), !!stack), (0, _defineProperty2.default)(_clsx, "".concat(prefixCls, "-stack-expanded"), expanded), _clsx)),
    style: style,
    keys: keys,
    motionAppear: true
  }, placementMotion, {
    onAllRemoved: function onAllRemoved() {
      onAllNoticeRemoved(placement);
    }
  }), function (_ref2, nodeRef) {
    var config = _ref2.config,
      motionClassName = _ref2.className,
      motionStyle = _ref2.style,
      motionIndex = _ref2.index;
    var _ref3 = config,
      key = _ref3.key,
      times = _ref3.times;
    var strKey = String(key);
    var _ref4 = config,
      configClassName = _ref4.className,
      configStyle = _ref4.style,
      configClassNames = _ref4.classNames,
      configStyles = _ref4.styles,
      restConfig = (0, _objectWithoutProperties2.default)(_ref4, _excluded);
    var dataIndex = keys.findIndex(function (item) {
      return item.key === strKey;
    });

    // If dataIndex is -1, that means this notice has been removed in data, but still in dom
    // Should minus (motionIndex - 1) to get the correct index because keys.length is not the same as dom length
    var stackStyle = {};
    if (stack) {
      var index = keys.length - 1 - (dataIndex > -1 ? dataIndex : motionIndex - 1);
      var transformX = placement === 'top' || placement === 'bottom' ? '-50%' : '0';
      if (index > 0) {
        var _dictRef$current$strK, _dictRef$current$strK2, _dictRef$current$strK3;
        stackStyle.height = expanded ? (_dictRef$current$strK = dictRef.current[strKey]) === null || _dictRef$current$strK === void 0 ? void 0 : _dictRef$current$strK.offsetHeight : latestNotice === null || latestNotice === void 0 ? void 0 : latestNotice.offsetHeight;

        // Transform
        var verticalOffset = 0;
        for (var i = 0; i < index; i++) {
          var _dictRef$current$keys;
          verticalOffset += ((_dictRef$current$keys = dictRef.current[keys[keys.length - 1 - i].key]) === null || _dictRef$current$keys === void 0 ? void 0 : _dictRef$current$keys.offsetHeight) + gap;
        }
        var transformY = (expanded ? verticalOffset : index * offset) * (placement.startsWith('top') ? 1 : -1);
        var scaleX = !expanded && latestNotice !== null && latestNotice !== void 0 && latestNotice.offsetWidth && (_dictRef$current$strK2 = dictRef.current[strKey]) !== null && _dictRef$current$strK2 !== void 0 && _dictRef$current$strK2.offsetWidth ? ((latestNotice === null || latestNotice === void 0 ? void 0 : latestNotice.offsetWidth) - offset * 2 * (index < 3 ? index : 3)) / ((_dictRef$current$strK3 = dictRef.current[strKey]) === null || _dictRef$current$strK3 === void 0 ? void 0 : _dictRef$current$strK3.offsetWidth) : 1;
        stackStyle.transform = "translate3d(".concat(transformX, ", ").concat(transformY, "px, 0) scaleX(").concat(scaleX, ")");
      } else {
        stackStyle.transform = "translate3d(".concat(transformX, ", 0, 0)");
      }
    }
    return /*#__PURE__*/_react.default.createElement("div", {
      ref: nodeRef,
      className: (0, _classnames.default)("".concat(prefixCls, "-notice-wrapper"), motionClassName, configClassNames === null || configClassNames === void 0 ? void 0 : configClassNames.wrapper),
      style: (0, _objectSpread2.default)((0, _objectSpread2.default)((0, _objectSpread2.default)({}, motionStyle), stackStyle), configStyles === null || configStyles === void 0 ? void 0 : configStyles.wrapper),
      onMouseEnter: function onMouseEnter() {
        return setHoverKeys(function (prev) {
          return prev.includes(strKey) ? prev : [].concat((0, _toConsumableArray2.default)(prev), [strKey]);
        });
      },
      onMouseLeave: function onMouseLeave() {
        return setHoverKeys(function (prev) {
          return prev.filter(function (k) {
            return k !== strKey;
          });
        });
      }
    }, /*#__PURE__*/_react.default.createElement(_Notice.default, (0, _extends2.default)({}, restConfig, {
      ref: function ref(node) {
        if (dataIndex > -1) {
          dictRef.current[strKey] = node;
        } else {
          delete dictRef.current[strKey];
        }
      },
      prefixCls: prefixCls,
      classNames: configClassNames,
      styles: configStyles,
      className: (0, _classnames.default)(configClassName, ctxCls === null || ctxCls === void 0 ? void 0 : ctxCls.notice),
      style: configStyle,
      times: times,
      key: key,
      eventKey: key,
      onNoticeClose: onNoticeClose,
      hovering: stack && hoverKeys.length > 0
    })));
  });
};
if (process.env.NODE_ENV !== 'production') {
  NoticeList.displayName = 'NoticeList';
}
var _default = exports.default = NoticeList;