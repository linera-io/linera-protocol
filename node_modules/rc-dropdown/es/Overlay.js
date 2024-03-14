import React, { forwardRef, useMemo } from 'react';
import { composeRef, supportRef } from "rc-util/es/ref";
var Overlay = /*#__PURE__*/forwardRef(function (props, ref) {
  var overlay = props.overlay,
    arrow = props.arrow,
    prefixCls = props.prefixCls;
  var overlayNode = useMemo(function () {
    var overlayElement;
    if (typeof overlay === 'function') {
      overlayElement = overlay();
    } else {
      overlayElement = overlay;
    }
    return overlayElement;
  }, [overlay]);
  var composedRef = composeRef(ref, overlayNode === null || overlayNode === void 0 ? void 0 : overlayNode.ref);
  return /*#__PURE__*/React.createElement(React.Fragment, null, arrow && /*#__PURE__*/React.createElement("div", {
    className: "".concat(prefixCls, "-arrow")
  }), /*#__PURE__*/React.cloneElement(overlayNode, {
    ref: supportRef(overlayNode) ? composedRef : undefined
  }));
});
export default Overlay;