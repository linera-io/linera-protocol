"use client";

import * as React from 'react';
import { devUseWarning } from '../_util/warning';
/**
 * Warning for ConfigProviderProps.
 * This will be empty function in production.
 */
const PropWarning = /*#__PURE__*/React.memo(_ref => {
  let {
    dropdownMatchSelectWidth
  } = _ref;
  const warning = devUseWarning('ConfigProvider');
  warning.deprecated(dropdownMatchSelectWidth === undefined, 'dropdownMatchSelectWidth', 'popupMatchSelectWidth');
  return null;
});
if (process.env.NODE_ENV !== 'production') {
  PropWarning.displayName = 'PropWarning';
}
export default process.env.NODE_ENV !== 'production' ? PropWarning : () => null;