import * as React from 'react';
var CacheContent = /*#__PURE__*/React.memo(function (_ref) {
  var children = _ref.children;
  return children;
}, function (_, next) {
  return !next.open;
});
if (process.env.NODE_ENV !== 'production') {
  CacheContent.displayName = 'CacheContent';
}
export default CacheContent;