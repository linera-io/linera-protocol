"use client";

import * as React from 'react';
const SizeContext = /*#__PURE__*/React.createContext(undefined);
export const SizeContextProvider = _ref => {
  let {
    children,
    size
  } = _ref;
  const originSize = React.useContext(SizeContext);
  return /*#__PURE__*/React.createElement(SizeContext.Provider, {
    value: size || originSize
  }, children);
};
export default SizeContext;