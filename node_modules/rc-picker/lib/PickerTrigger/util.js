"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.pickTriggerProps = pickTriggerProps;
var _miscUtil = require("../utils/miscUtil");
function pickTriggerProps(props) {
  return (0, _miscUtil.pickProps)(props, ['placement', 'builtinPlacements', 'popupAlign', 'getPopupContainer', 'transitionName', 'direction']);
}