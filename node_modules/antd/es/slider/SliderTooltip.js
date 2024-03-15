"use client";

import raf from "rc-util/es/raf";
import { composeRef } from "rc-util/es/ref";
import * as React from 'react';
import { useRef } from 'react';
import Tooltip from '../tooltip';
const SliderTooltip = /*#__PURE__*/React.forwardRef((props, ref) => {
  const {
    open
  } = props;
  const innerRef = useRef(null);
  const rafRef = useRef(null);
  function cancelKeepAlign() {
    raf.cancel(rafRef.current);
    rafRef.current = null;
  }
  function keepAlign() {
    rafRef.current = raf(() => {
      var _a;
      (_a = innerRef.current) === null || _a === void 0 ? void 0 : _a.forceAlign();
      rafRef.current = null;
    });
  }
  React.useEffect(() => {
    if (open) {
      keepAlign();
    } else {
      cancelKeepAlign();
    }
    return cancelKeepAlign;
  }, [open, props.title]);
  return /*#__PURE__*/React.createElement(Tooltip, Object.assign({
    ref: composeRef(innerRef, ref)
  }, props));
});
if (process.env.NODE_ENV !== 'production') {
  SliderTooltip.displayName = 'SliderTooltip';
}
export default SliderTooltip;