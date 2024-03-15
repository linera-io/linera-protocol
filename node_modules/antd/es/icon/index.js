"use client";

import { devUseWarning } from '../_util/warning';
const Icon = () => {
  if (process.env.NODE_ENV !== 'production') {
    const warning = devUseWarning('Icon');
    process.env.NODE_ENV !== "production" ? warning(false, 'usage', 'Empty Icon') : void 0;
  }
  return null;
};
export default Icon;