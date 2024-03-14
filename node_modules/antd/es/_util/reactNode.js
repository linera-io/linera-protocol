import React from 'react';
export function isFragment(child) {
  return child && /*#__PURE__*/React.isValidElement(child) && child.type === React.Fragment;
}
export const replaceElement = (element, replacement, props) => {
  if (! /*#__PURE__*/React.isValidElement(element)) {
    return replacement;
  }
  return /*#__PURE__*/React.cloneElement(element, typeof props === 'function' ? props(element.props || {}) : props);
};
export function cloneElement(element, props) {
  return replaceElement(element, element, props);
}