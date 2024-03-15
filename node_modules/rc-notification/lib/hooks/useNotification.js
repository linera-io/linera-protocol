"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");
var _typeof = require("@babel/runtime/helpers/typeof");
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = useNotification;
var _toConsumableArray2 = _interopRequireDefault(require("@babel/runtime/helpers/toConsumableArray"));
var _slicedToArray2 = _interopRequireDefault(require("@babel/runtime/helpers/slicedToArray"));
var _objectWithoutProperties2 = _interopRequireDefault(require("@babel/runtime/helpers/objectWithoutProperties"));
var React = _interopRequireWildcard(require("react"));
var _Notifications = _interopRequireDefault(require("../Notifications"));
var _excluded = ["getContainer", "motion", "prefixCls", "maxCount", "className", "style", "onAllRemoved", "stack", "renderNotifications"];
function _getRequireWildcardCache(e) { if ("function" != typeof WeakMap) return null; var r = new WeakMap(), t = new WeakMap(); return (_getRequireWildcardCache = function _getRequireWildcardCache(e) { return e ? t : r; })(e); }
function _interopRequireWildcard(e, r) { if (!r && e && e.__esModule) return e; if (null === e || "object" != _typeof(e) && "function" != typeof e) return { default: e }; var t = _getRequireWildcardCache(r); if (t && t.has(e)) return t.get(e); var n = { __proto__: null }, a = Object.defineProperty && Object.getOwnPropertyDescriptor; for (var u in e) if ("default" !== u && Object.prototype.hasOwnProperty.call(e, u)) { var i = a ? Object.getOwnPropertyDescriptor(e, u) : null; i && (i.get || i.set) ? Object.defineProperty(n, u, i) : n[u] = e[u]; } return n.default = e, t && t.set(e, n), n; }
var defaultGetContainer = function defaultGetContainer() {
  return document.body;
};
var uniqueKey = 0;
function mergeConfig() {
  var clone = {};
  for (var _len = arguments.length, objList = new Array(_len), _key = 0; _key < _len; _key++) {
    objList[_key] = arguments[_key];
  }
  objList.forEach(function (obj) {
    if (obj) {
      Object.keys(obj).forEach(function (key) {
        var val = obj[key];
        if (val !== undefined) {
          clone[key] = val;
        }
      });
    }
  });
  return clone;
}
function useNotification() {
  var rootConfig = arguments.length > 0 && arguments[0] !== undefined ? arguments[0] : {};
  var _rootConfig$getContai = rootConfig.getContainer,
    getContainer = _rootConfig$getContai === void 0 ? defaultGetContainer : _rootConfig$getContai,
    motion = rootConfig.motion,
    prefixCls = rootConfig.prefixCls,
    maxCount = rootConfig.maxCount,
    className = rootConfig.className,
    style = rootConfig.style,
    onAllRemoved = rootConfig.onAllRemoved,
    stack = rootConfig.stack,
    renderNotifications = rootConfig.renderNotifications,
    shareConfig = (0, _objectWithoutProperties2.default)(rootConfig, _excluded);
  var _React$useState = React.useState(),
    _React$useState2 = (0, _slicedToArray2.default)(_React$useState, 2),
    container = _React$useState2[0],
    setContainer = _React$useState2[1];
  var notificationsRef = React.useRef();
  var contextHolder = /*#__PURE__*/React.createElement(_Notifications.default, {
    container: container,
    ref: notificationsRef,
    prefixCls: prefixCls,
    motion: motion,
    maxCount: maxCount,
    className: className,
    style: style,
    onAllRemoved: onAllRemoved,
    stack: stack,
    renderNotifications: renderNotifications
  });
  var _React$useState3 = React.useState([]),
    _React$useState4 = (0, _slicedToArray2.default)(_React$useState3, 2),
    taskQueue = _React$useState4[0],
    setTaskQueue = _React$useState4[1];

  // ========================= Refs =========================
  var api = React.useMemo(function () {
    return {
      open: function open(config) {
        var mergedConfig = mergeConfig(shareConfig, config);
        if (mergedConfig.key === null || mergedConfig.key === undefined) {
          mergedConfig.key = "rc-notification-".concat(uniqueKey);
          uniqueKey += 1;
        }
        setTaskQueue(function (queue) {
          return [].concat((0, _toConsumableArray2.default)(queue), [{
            type: 'open',
            config: mergedConfig
          }]);
        });
      },
      close: function close(key) {
        setTaskQueue(function (queue) {
          return [].concat((0, _toConsumableArray2.default)(queue), [{
            type: 'close',
            key: key
          }]);
        });
      },
      destroy: function destroy() {
        setTaskQueue(function (queue) {
          return [].concat((0, _toConsumableArray2.default)(queue), [{
            type: 'destroy'
          }]);
        });
      }
    };
  }, []);

  // ======================= Container ======================
  // React 18 should all in effect that we will check container in each render
  // Which means getContainer should be stable.
  React.useEffect(function () {
    setContainer(getContainer());
  });

  // ======================== Effect ========================
  React.useEffect(function () {
    // Flush task when node ready
    if (notificationsRef.current && taskQueue.length) {
      taskQueue.forEach(function (task) {
        switch (task.type) {
          case 'open':
            notificationsRef.current.open(task.config);
            break;
          case 'close':
            notificationsRef.current.close(task.key);
            break;
          case 'destroy':
            notificationsRef.current.destroy();
            break;
        }
      });

      // React 17 will mix order of effect & setState in async
      // - open: setState[0]
      // - effect[0]
      // - open: setState[1]
      // - effect setState([]) * here will clean up [0, 1] in React 17
      setTaskQueue(function (oriQueue) {
        return oriQueue.filter(function (task) {
          return !taskQueue.includes(task);
        });
      });
    }
  }, [taskQueue]);

  // ======================== Return ========================
  return [api, contextHolder];
}