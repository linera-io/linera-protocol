/**
 * BaseSelect provide some parsed data into context.
 * You can use this hooks to get them.
 */

import * as React from 'react';
export var BaseSelectContext = /*#__PURE__*/React.createContext(null);
export default function useBaseProps() {
  return React.useContext(BaseSelectContext);
}