import _extends from "@babel/runtime/helpers/esm/extends";
import _objectSpread from "@babel/runtime/helpers/esm/objectSpread2";
import _objectWithoutProperties from "@babel/runtime/helpers/esm/objectWithoutProperties";
var _excluded = ["renderTabBar"],
  _excluded2 = ["label", "key"];
// zombieJ: To compatible with `renderTabBar` usage.

import * as React from 'react';
import TabNavList from '.';
import TabContext from "../TabContext";
import TabPane from "../TabPanelList/TabPane";
// We have to create a TabNavList components.
var TabNavListWrapper = function TabNavListWrapper(_ref) {
  var renderTabBar = _ref.renderTabBar,
    restProps = _objectWithoutProperties(_ref, _excluded);
  var _React$useContext = React.useContext(TabContext),
    tabs = _React$useContext.tabs;
  if (renderTabBar) {
    var tabNavBarProps = _objectSpread(_objectSpread({}, restProps), {}, {
      // Legacy support. We do not use this actually
      panes: tabs.map(function (_ref2) {
        var label = _ref2.label,
          key = _ref2.key,
          restTabProps = _objectWithoutProperties(_ref2, _excluded2);
        return /*#__PURE__*/React.createElement(TabPane, _extends({
          tab: label,
          key: key,
          tabKey: key
        }, restTabProps));
      })
    });
    return renderTabBar(tabNavBarProps, TabNavList);
  }
  return /*#__PURE__*/React.createElement(TabNavList, restProps);
};
if (process.env.NODE_ENV !== 'production') {
  TabNavListWrapper.displayName = 'TabNavListWrapper';
}
export default TabNavListWrapper;