import * as React from 'react';
export var IdContext = /*#__PURE__*/React.createContext(null);
export function getMenuId(uuid, eventKey) {
  if (uuid === undefined) {
    return null;
  }
  return "".concat(uuid, "-").concat(eventKey);
}

/**
 * Get `data-menu-id`
 */
export function useMenuId(eventKey) {
  var id = React.useContext(IdContext);
  return getMenuId(id, eventKey);
}