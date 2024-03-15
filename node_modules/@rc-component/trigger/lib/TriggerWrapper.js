"use strict";

var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _ref = require("rc-util/lib/ref");
var React = _interopRequireWildcard(require("react"));
var TriggerWrapper = /*#__PURE__*/React.forwardRef(function (props, ref) {
  var children = props.children,
    getTriggerDOMNode = props.getTriggerDOMNode;
  var canUseRef = (0, _ref.supportRef)(children);

  // When use `getTriggerDOMNode`, we should do additional work to get the real dom
  var setRef = React.useCallback(function (node) {
    (0, _ref.fillRef)(ref, getTriggerDOMNode ? getTriggerDOMNode(node) : node);
  }, [getTriggerDOMNode]);
  var mergedRef = (0, _ref.useComposeRef)(setRef, children.ref);
  return canUseRef ? /*#__PURE__*/React.cloneElement(children, {
    ref: mergedRef
  }) : children;
});
if (process.env.NODE_ENV !== 'production') {
  TriggerWrapper.displayName = 'TriggerWrapper';
}
var _default = exports.default = TriggerWrapper;