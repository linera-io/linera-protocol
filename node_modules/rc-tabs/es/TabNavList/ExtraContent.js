import _typeof from "@babel/runtime/helpers/esm/typeof";
import * as React from 'react';
var ExtraContent = /*#__PURE__*/React.forwardRef(function (props, ref) {
  var position = props.position,
    prefixCls = props.prefixCls,
    extra = props.extra;
  if (!extra) {
    return null;
  }
  var content;

  // Parse extra
  var assertExtra = {};
  if (_typeof(extra) === 'object' && ! /*#__PURE__*/React.isValidElement(extra)) {
    assertExtra = extra;
  } else {
    assertExtra.right = extra;
  }
  if (position === 'right') {
    content = assertExtra.right;
  }
  if (position === 'left') {
    content = assertExtra.left;
  }
  return content ? /*#__PURE__*/React.createElement("div", {
    className: "".concat(prefixCls, "-extra-content"),
    ref: ref
  }, content) : null;
});
if (process.env.NODE_ENV !== 'production') {
  ExtraContent.displayName = 'ExtraContent';
}
export default ExtraContent;