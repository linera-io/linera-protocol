"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");
var _typeof = require("@babel/runtime/helpers/typeof");
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = createImmutable;
var _extends2 = _interopRequireDefault(require("@babel/runtime/helpers/extends"));
var _ref = require("rc-util/lib/ref");
var React = _interopRequireWildcard(require("react"));
function _getRequireWildcardCache(nodeInterop) { if (typeof WeakMap !== "function") return null; var cacheBabelInterop = new WeakMap(); var cacheNodeInterop = new WeakMap(); return (_getRequireWildcardCache = function _getRequireWildcardCache(nodeInterop) { return nodeInterop ? cacheNodeInterop : cacheBabelInterop; })(nodeInterop); }
function _interopRequireWildcard(obj, nodeInterop) { if (!nodeInterop && obj && obj.__esModule) { return obj; } if (obj === null || _typeof(obj) !== "object" && typeof obj !== "function") { return { default: obj }; } var cache = _getRequireWildcardCache(nodeInterop); if (cache && cache.has(obj)) { return cache.get(obj); } var newObj = {}; var hasPropertyDescriptor = Object.defineProperty && Object.getOwnPropertyDescriptor; for (var key in obj) { if (key !== "default" && Object.prototype.hasOwnProperty.call(obj, key)) { var desc = hasPropertyDescriptor ? Object.getOwnPropertyDescriptor(obj, key) : null; if (desc && (desc.get || desc.set)) { Object.defineProperty(newObj, key, desc); } else { newObj[key] = obj[key]; } } } newObj.default = obj; if (cache) { cache.set(obj, newObj); } return newObj; }
/**
 * Create Immutable pair for `makeImmutable` and `responseImmutable`.
 */
function createImmutable() {
  var ImmutableContext = /*#__PURE__*/React.createContext(null);

  /**
   * Get render update mark by `makeImmutable` root.
   * Do not deps on the return value as render times
   * but only use for `useMemo` or `useCallback` deps.
   */
  function useImmutableMark() {
    return React.useContext(ImmutableContext);
  }

  /**
  * Wrapped Component will be marked as Immutable.
  * When Component parent trigger render,
  * it will notice children component (use with `responseImmutable`) node that parent has updated.
  * @param Component Passed Component
  * @param triggerRender Customize trigger `responseImmutable` children re-render logic. Default will always trigger re-render when this component re-render.
  */
  function makeImmutable(Component, shouldTriggerRender) {
    var refAble = (0, _ref.supportRef)(Component);
    var ImmutableComponent = function ImmutableComponent(props, ref) {
      var refProps = refAble ? {
        ref: ref
      } : {};
      var renderTimesRef = React.useRef(0);
      var prevProps = React.useRef(props);

      // If parent has the context, we do not wrap it
      var mark = useImmutableMark();
      if (mark !== null) {
        return /*#__PURE__*/React.createElement(Component, (0, _extends2.default)({}, props, refProps));
      }
      if (
      // Always trigger re-render if not provide `notTriggerRender`
      !shouldTriggerRender || shouldTriggerRender(prevProps.current, props)) {
        renderTimesRef.current += 1;
      }
      prevProps.current = props;
      return /*#__PURE__*/React.createElement(ImmutableContext.Provider, {
        value: renderTimesRef.current
      }, /*#__PURE__*/React.createElement(Component, (0, _extends2.default)({}, props, refProps)));
    };
    if (process.env.NODE_ENV !== 'production') {
      ImmutableComponent.displayName = "ImmutableRoot(".concat(Component.displayName || Component.name, ")");
    }
    return refAble ? /*#__PURE__*/React.forwardRef(ImmutableComponent) : ImmutableComponent;
  }

  /**
   * Wrapped Component with `React.memo`.
   * But will rerender when parent with `makeImmutable` rerender.
   */
  function responseImmutable(Component, propsAreEqual) {
    var refAble = (0, _ref.supportRef)(Component);
    var ImmutableComponent = function ImmutableComponent(props, ref) {
      var refProps = refAble ? {
        ref: ref
      } : {};
      useImmutableMark();
      return /*#__PURE__*/React.createElement(Component, (0, _extends2.default)({}, props, refProps));
    };
    if (process.env.NODE_ENV !== 'production') {
      ImmutableComponent.displayName = "ImmutableResponse(".concat(Component.displayName || Component.name, ")");
    }
    return refAble ? /*#__PURE__*/React.memo( /*#__PURE__*/React.forwardRef(ImmutableComponent), propsAreEqual) : /*#__PURE__*/React.memo(ImmutableComponent, propsAreEqual);
  }
  return {
    makeImmutable: makeImmutable,
    responseImmutable: responseImmutable,
    useImmutableMark: useImmutableMark
  };
}