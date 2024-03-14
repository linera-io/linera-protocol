import React from 'react';
export var NotificationContext = /*#__PURE__*/React.createContext({});
var NotificationProvider = function NotificationProvider(_ref) {
  var children = _ref.children,
    classNames = _ref.classNames;
  return /*#__PURE__*/React.createElement(NotificationContext.Provider, {
    value: {
      classNames: classNames
    }
  }, children);
};
export default NotificationProvider;