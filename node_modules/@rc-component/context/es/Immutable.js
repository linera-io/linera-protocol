import _extends from "@babel/runtime/helpers/esm/extends";
import { supportRef } from "rc-util/es/ref";
import * as React from 'react';
/**
 * Create Immutable pair for `makeImmutable` and `responseImmutable`.
 */
export default function createImmutable() {
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
    var refAble = supportRef(Component);
    var ImmutableComponent = function ImmutableComponent(props, ref) {
      var refProps = refAble ? {
        ref: ref
      } : {};
      var renderTimesRef = React.useRef(0);
      var prevProps = React.useRef(props);

      // If parent has the context, we do not wrap it
      var mark = useImmutableMark();
      if (mark !== null) {
        return /*#__PURE__*/React.createElement(Component, _extends({}, props, refProps));
      }
      if (
      // Always trigger re-render if not provide `notTriggerRender`
      !shouldTriggerRender || shouldTriggerRender(prevProps.current, props)) {
        renderTimesRef.current += 1;
      }
      prevProps.current = props;
      return /*#__PURE__*/React.createElement(ImmutableContext.Provider, {
        value: renderTimesRef.current
      }, /*#__PURE__*/React.createElement(Component, _extends({}, props, refProps)));
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
    var refAble = supportRef(Component);
    var ImmutableComponent = function ImmutableComponent(props, ref) {
      var refProps = refAble ? {
        ref: ref
      } : {};
      useImmutableMark();
      return /*#__PURE__*/React.createElement(Component, _extends({}, props, refProps));
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