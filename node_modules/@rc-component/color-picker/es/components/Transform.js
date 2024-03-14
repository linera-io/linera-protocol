import React, { forwardRef } from 'react';
var Transform = /*#__PURE__*/forwardRef(function (props, ref) {
  var children = props.children,
    offset = props.offset;
  return /*#__PURE__*/React.createElement("div", {
    ref: ref,
    style: {
      position: 'absolute',
      left: offset.x,
      top: offset.y,
      zIndex: 1
    }
  }, children);
});
export default Transform;