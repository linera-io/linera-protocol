import * as React from 'react';
import { MenuContext } from "../context/MenuContext";
export default function useActive(eventKey, disabled, onMouseEnter, onMouseLeave) {
  var _React$useContext = React.useContext(MenuContext),
    activeKey = _React$useContext.activeKey,
    onActive = _React$useContext.onActive,
    onInactive = _React$useContext.onInactive;
  var ret = {
    active: activeKey === eventKey
  };

  // Skip when disabled
  if (!disabled) {
    ret.onMouseEnter = function (domEvent) {
      onMouseEnter === null || onMouseEnter === void 0 || onMouseEnter({
        key: eventKey,
        domEvent: domEvent
      });
      onActive(eventKey);
    };
    ret.onMouseLeave = function (domEvent) {
      onMouseLeave === null || onMouseLeave === void 0 || onMouseLeave({
        key: eventKey,
        domEvent: domEvent
      });
      onInactive(eventKey);
    };
  }
  return ret;
}