import * as React from 'react';
var PopupContent = /*#__PURE__*/React.memo(function (_ref) {
  var children = _ref.children;
  return children;
}, function (_, next) {
  return next.cache;
});
if (process.env.NODE_ENV !== 'production') {
  PopupContent.displayName = 'PopupContent';
}
export default PopupContent;