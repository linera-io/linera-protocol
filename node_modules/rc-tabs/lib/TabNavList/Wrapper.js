"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");
var _typeof = require("@babel/runtime/helpers/typeof");
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _extends2 = _interopRequireDefault(require("@babel/runtime/helpers/extends"));
var _objectSpread2 = _interopRequireDefault(require("@babel/runtime/helpers/objectSpread2"));
var _objectWithoutProperties2 = _interopRequireDefault(require("@babel/runtime/helpers/objectWithoutProperties"));
var React = _interopRequireWildcard(require("react"));
var _ = _interopRequireDefault(require("."));
var _TabContext = _interopRequireDefault(require("../TabContext"));
var _TabPane = _interopRequireDefault(require("../TabPanelList/TabPane"));
var _excluded = ["renderTabBar"],
  _excluded2 = ["label", "key"]; // zombieJ: To compatible with `renderTabBar` usage.
function _getRequireWildcardCache(e) { if ("function" != typeof WeakMap) return null; var r = new WeakMap(), t = new WeakMap(); return (_getRequireWildcardCache = function _getRequireWildcardCache(e) { return e ? t : r; })(e); }
function _interopRequireWildcard(e, r) { if (!r && e && e.__esModule) return e; if (null === e || "object" != _typeof(e) && "function" != typeof e) return { default: e }; var t = _getRequireWildcardCache(r); if (t && t.has(e)) return t.get(e); var n = { __proto__: null }, a = Object.defineProperty && Object.getOwnPropertyDescriptor; for (var u in e) if ("default" !== u && Object.prototype.hasOwnProperty.call(e, u)) { var i = a ? Object.getOwnPropertyDescriptor(e, u) : null; i && (i.get || i.set) ? Object.defineProperty(n, u, i) : n[u] = e[u]; } return n.default = e, t && t.set(e, n), n; }
// We have to create a TabNavList components.
var TabNavListWrapper = function TabNavListWrapper(_ref) {
  var renderTabBar = _ref.renderTabBar,
    restProps = (0, _objectWithoutProperties2.default)(_ref, _excluded);
  var _React$useContext = React.useContext(_TabContext.default),
    tabs = _React$useContext.tabs;
  if (renderTabBar) {
    var tabNavBarProps = (0, _objectSpread2.default)((0, _objectSpread2.default)({}, restProps), {}, {
      // Legacy support. We do not use this actually
      panes: tabs.map(function (_ref2) {
        var label = _ref2.label,
          key = _ref2.key,
          restTabProps = (0, _objectWithoutProperties2.default)(_ref2, _excluded2);
        return /*#__PURE__*/React.createElement(_TabPane.default, (0, _extends2.default)({
          tab: label,
          key: key,
          tabKey: key
        }, restTabProps));
      })
    });
    return renderTabBar(tabNavBarProps, _.default);
  }
  return /*#__PURE__*/React.createElement(_.default, restProps);
};
if (process.env.NODE_ENV !== 'production') {
  TabNavListWrapper.displayName = 'TabNavListWrapper';
}
var _default = exports.default = TabNavListWrapper;