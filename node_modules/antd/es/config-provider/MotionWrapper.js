"use client";

import { Provider as MotionProvider } from 'rc-motion';
import * as React from 'react';
import { useToken } from '../theme/internal';
export default function MotionWrapper(props) {
  const {
    children
  } = props;
  const [, token] = useToken();
  const {
    motion
  } = token;
  const needWrapMotionProviderRef = React.useRef(false);
  needWrapMotionProviderRef.current = needWrapMotionProviderRef.current || motion === false;
  if (needWrapMotionProviderRef.current) {
    return /*#__PURE__*/React.createElement(MotionProvider, {
      motion: motion
    }, children);
  }
  return children;
}