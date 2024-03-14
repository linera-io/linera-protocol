"use strict";

var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
exports.usePanelRef = usePanelRef;
var _rcUtil = require("rc-util");
var React = _interopRequireWildcard(require("react"));
function voidFunc() {}
const WatermarkContext = /*#__PURE__*/React.createContext({
  add: voidFunc,
  remove: voidFunc
});
function usePanelRef(panelSelector) {
  const watermark = React.useContext(WatermarkContext);
  const panelEleRef = React.useRef();
  const panelRef = (0, _rcUtil.useEvent)(ele => {
    if (ele) {
      const innerContentEle = panelSelector ? ele.querySelector(panelSelector) : ele;
      watermark.add(innerContentEle);
      panelEleRef.current = innerContentEle;
    } else {
      watermark.remove(panelEleRef.current);
    }
  });
  return panelRef;
}
var _default = exports.default = WatermarkContext;