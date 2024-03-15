import * as React from 'react';
import toArray from "rc-util/es/Children/toArray";
import { devUseWarning } from '../_util/warning';
function filter(items) {
  return items.filter(item => item);
}
export default function useLegacyItems(items, children) {
  if (process.env.NODE_ENV === 'test') {
    const warning = devUseWarning('Menu');
    warning.deprecated(!children, 'Step', 'items');
  }
  if (items) {
    return items;
  }
  const childrenItems = toArray(children).map(node => {
    if ( /*#__PURE__*/React.isValidElement(node)) {
      const {
        props
      } = node;
      const item = Object.assign({}, props);
      return item;
    }
    return null;
  });
  return filter(childrenItems);
}