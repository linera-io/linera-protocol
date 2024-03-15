"use strict";

var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard").default;
var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _slicedToArray2 = _interopRequireDefault(require("@babel/runtime/helpers/slicedToArray"));
var React = _interopRequireWildcard(require("react"));
var _reactDom = require("react-dom");
var _canUseDom = _interopRequireDefault(require("rc-util/lib/Dom/canUseDom"));
var _warning = _interopRequireDefault(require("rc-util/lib/warning"));
var _ref2 = require("rc-util/lib/ref");
var _Context = _interopRequireDefault(require("./Context"));
var _useDom3 = _interopRequireDefault(require("./useDom"));
var _useScrollLocker = _interopRequireDefault(require("./useScrollLocker"));
var _mock = require("./mock");
var getPortalContainer = function getPortalContainer(getContainer) {
  if (getContainer === false) {
    return false;
  }
  if (!(0, _canUseDom.default)() || !getContainer) {
    return null;
  }
  if (typeof getContainer === 'string') {
    return document.querySelector(getContainer);
  }
  if (typeof getContainer === 'function') {
    return getContainer();
  }
  return getContainer;
};
var Portal = /*#__PURE__*/React.forwardRef(function (props, ref) {
  var open = props.open,
    autoLock = props.autoLock,
    getContainer = props.getContainer,
    debug = props.debug,
    _props$autoDestroy = props.autoDestroy,
    autoDestroy = _props$autoDestroy === void 0 ? true : _props$autoDestroy,
    children = props.children;
  var _React$useState = React.useState(open),
    _React$useState2 = (0, _slicedToArray2.default)(_React$useState, 2),
    shouldRender = _React$useState2[0],
    setShouldRender = _React$useState2[1];
  var mergedRender = shouldRender || open;

  // ========================= Warning =========================
  if (process.env.NODE_ENV !== 'production') {
    (0, _warning.default)((0, _canUseDom.default)() || !open, "Portal only work in client side. Please call 'useEffect' to show Portal instead default render in SSR.");
  }

  // ====================== Should Render ======================
  React.useEffect(function () {
    if (autoDestroy || open) {
      setShouldRender(open);
    }
  }, [open, autoDestroy]);

  // ======================== Container ========================
  var _React$useState3 = React.useState(function () {
      return getPortalContainer(getContainer);
    }),
    _React$useState4 = (0, _slicedToArray2.default)(_React$useState3, 2),
    innerContainer = _React$useState4[0],
    setInnerContainer = _React$useState4[1];
  React.useEffect(function () {
    var customizeContainer = getPortalContainer(getContainer);

    // Tell component that we check this in effect which is safe to be `null`
    setInnerContainer(customizeContainer !== null && customizeContainer !== void 0 ? customizeContainer : null);
  });
  var _useDom = (0, _useDom3.default)(mergedRender && !innerContainer, debug),
    _useDom2 = (0, _slicedToArray2.default)(_useDom, 2),
    defaultContainer = _useDom2[0],
    queueCreate = _useDom2[1];
  var mergedContainer = innerContainer !== null && innerContainer !== void 0 ? innerContainer : defaultContainer;

  // ========================= Locker ==========================
  (0, _useScrollLocker.default)(autoLock && open && (0, _canUseDom.default)() && (mergedContainer === defaultContainer || mergedContainer === document.body));

  // =========================== Ref ===========================
  var childRef = null;
  if (children && (0, _ref2.supportRef)(children) && ref) {
    var _ref = children;
    childRef = _ref.ref;
  }
  var mergedRef = (0, _ref2.useComposeRef)(childRef, ref);

  // ========================= Render ==========================
  // Do not render when nothing need render
  // When innerContainer is `undefined`, it may not ready since user use ref in the same render
  if (!mergedRender || !(0, _canUseDom.default)() || innerContainer === undefined) {
    return null;
  }

  // Render inline
  var renderInline = mergedContainer === false || (0, _mock.inlineMock)();
  var reffedChildren = children;
  if (ref) {
    reffedChildren = /*#__PURE__*/React.cloneElement(children, {
      ref: mergedRef
    });
  }
  return /*#__PURE__*/React.createElement(_Context.default.Provider, {
    value: queueCreate
  }, renderInline ? reffedChildren : /*#__PURE__*/(0, _reactDom.createPortal)(reffedChildren, mergedContainer));
});
if (process.env.NODE_ENV !== 'production') {
  Portal.displayName = 'Portal';
}
var _default = Portal;
exports.default = _default;