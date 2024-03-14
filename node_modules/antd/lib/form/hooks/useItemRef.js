"use strict";

var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = useItemRef;
var _ref = require("rc-util/lib/ref");
var React = _interopRequireWildcard(require("react"));
var _context = require("../context");
function useItemRef() {
  const {
    itemRef
  } = React.useContext(_context.FormContext);
  const cacheRef = React.useRef({});
  function getRef(name, children) {
    const childrenRef = children && typeof children === 'object' && children.ref;
    const nameStr = name.join('_');
    if (cacheRef.current.name !== nameStr || cacheRef.current.originRef !== childrenRef) {
      cacheRef.current.name = nameStr;
      cacheRef.current.originRef = childrenRef;
      cacheRef.current.ref = (0, _ref.composeRef)(itemRef(name), childrenRef);
    }
    return cacheRef.current.ref;
  }
  return getRef;
}