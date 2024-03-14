import { fillRef, supportRef, useComposeRef } from "rc-util/es/ref";
import * as React from 'react';
var TriggerWrapper = /*#__PURE__*/React.forwardRef(function (props, ref) {
  var children = props.children,
    getTriggerDOMNode = props.getTriggerDOMNode;
  var canUseRef = supportRef(children);

  // When use `getTriggerDOMNode`, we should do additional work to get the real dom
  var setRef = React.useCallback(function (node) {
    fillRef(ref, getTriggerDOMNode ? getTriggerDOMNode(node) : node);
  }, [getTriggerDOMNode]);
  var mergedRef = useComposeRef(setRef, children.ref);
  return canUseRef ? /*#__PURE__*/React.cloneElement(children, {
    ref: mergedRef
  }) : children;
});
if (process.env.NODE_ENV !== 'production') {
  TriggerWrapper.displayName = 'TriggerWrapper';
}
export default TriggerWrapper;