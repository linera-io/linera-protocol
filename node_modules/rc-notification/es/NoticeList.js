import _extends from "@babel/runtime/helpers/esm/extends";
import _toConsumableArray from "@babel/runtime/helpers/esm/toConsumableArray";
import _objectSpread from "@babel/runtime/helpers/esm/objectSpread2";
import _objectWithoutProperties from "@babel/runtime/helpers/esm/objectWithoutProperties";
import _defineProperty from "@babel/runtime/helpers/esm/defineProperty";
import _slicedToArray from "@babel/runtime/helpers/esm/slicedToArray";
var _excluded = ["className", "style", "classNames", "styles"];
import React, { useContext, useEffect, useRef, useState } from 'react';
import clsx from 'classnames';
import { CSSMotionList } from 'rc-motion';
import Notice from "./Notice";
import { NotificationContext } from "./NotificationProvider";
import useStack from "./hooks/useStack";
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
  var _useContext = useContext(NotificationContext),
    ctxCls = _useContext.classNames;
  var dictRef = useRef({});
  var _useState = useState(null),
    _useState2 = _slicedToArray(_useState, 2),
    latestNotice = _useState2[0],
    setLatestNotice = _useState2[1];
  var _useState3 = useState([]),
    _useState4 = _slicedToArray(_useState3, 2),
    hoverKeys = _useState4[0],
    setHoverKeys = _useState4[1];
  var keys = configList.map(function (config) {
    return {
      config: config,
      key: String(config.key)
    };
  });
  var _useStack = useStack(stackConfig),
    _useStack2 = _slicedToArray(_useStack, 2),
    stack = _useStack2[0],
    _useStack2$ = _useStack2[1],
    offset = _useStack2$.offset,
    threshold = _useStack2$.threshold,
    gap = _useStack2$.gap;
  var expanded = stack && (hoverKeys.length > 0 || keys.length <= threshold);
  var placementMotion = typeof motion === 'function' ? motion(placement) : motion;

  // Clean hover key
  useEffect(function () {
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
  useEffect(function () {
    var _keys;
    if (stack && dictRef.current[(_keys = keys[keys.length - 1]) === null || _keys === void 0 ? void 0 : _keys.key]) {
      var _keys2;
      setLatestNotice(dictRef.current[(_keys2 = keys[keys.length - 1]) === null || _keys2 === void 0 ? void 0 : _keys2.key]);
    }
  }, [keys, stack]);
  return /*#__PURE__*/React.createElement(CSSMotionList, _extends({
    key: placement,
    className: clsx(prefixCls, "".concat(prefixCls, "-").concat(placement), ctxCls === null || ctxCls === void 0 ? void 0 : ctxCls.list, className, (_clsx = {}, _defineProperty(_clsx, "".concat(prefixCls, "-stack"), !!stack), _defineProperty(_clsx, "".concat(prefixCls, "-stack-expanded"), expanded), _clsx)),
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
      restConfig = _objectWithoutProperties(_ref4, _excluded);
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
    return /*#__PURE__*/React.createElement("div", {
      ref: nodeRef,
      className: clsx("".concat(prefixCls, "-notice-wrapper"), motionClassName, configClassNames === null || configClassNames === void 0 ? void 0 : configClassNames.wrapper),
      style: _objectSpread(_objectSpread(_objectSpread({}, motionStyle), stackStyle), configStyles === null || configStyles === void 0 ? void 0 : configStyles.wrapper),
      onMouseEnter: function onMouseEnter() {
        return setHoverKeys(function (prev) {
          return prev.includes(strKey) ? prev : [].concat(_toConsumableArray(prev), [strKey]);
        });
      },
      onMouseLeave: function onMouseLeave() {
        return setHoverKeys(function (prev) {
          return prev.filter(function (k) {
            return k !== strKey;
          });
        });
      }
    }, /*#__PURE__*/React.createElement(Notice, _extends({}, restConfig, {
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
      className: clsx(configClassName, ctxCls === null || ctxCls === void 0 ? void 0 : ctxCls.notice),
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
export default NoticeList;