import _slicedToArray from "@babel/runtime/helpers/esm/slicedToArray";
import useEvent from "rc-util/es/hooks/useEvent";
import useLayoutEffect from "rc-util/es/hooks/useLayoutEffect";
import isEqual from "rc-util/es/isEqual";
import * as React from 'react';
import { unstable_batchedUpdates } from 'react-dom';
export function createContext(defaultValue) {
  var Context = /*#__PURE__*/React.createContext(undefined);
  var Provider = function Provider(_ref) {
    var value = _ref.value,
      children = _ref.children;
    var valueRef = React.useRef(value);
    valueRef.current = value;
    var _React$useState = React.useState(function () {
        return {
          getValue: function getValue() {
            return valueRef.current;
          },
          listeners: new Set()
        };
      }),
      _React$useState2 = _slicedToArray(_React$useState, 1),
      context = _React$useState2[0];
    useLayoutEffect(function () {
      unstable_batchedUpdates(function () {
        context.listeners.forEach(function (listener) {
          listener(value);
        });
      });
    }, [value]);
    return /*#__PURE__*/React.createElement(Context.Provider, {
      value: context
    }, children);
  };
  return {
    Context: Context,
    Provider: Provider,
    defaultValue: defaultValue
  };
}

/** e.g. useSelect(userContext) => user */

/** e.g. useSelect(userContext, user => user.name) => user.name */

/** e.g. useSelect(userContext, ['name', 'age']) => user { name, age } */

/** e.g. useSelect(userContext, 'name') => user.name */

export function useContext(holder, selector) {
  var eventSelector = useEvent(typeof selector === 'function' ? selector : function (ctx) {
    if (selector === undefined) {
      return ctx;
    }
    if (!Array.isArray(selector)) {
      return ctx[selector];
    }
    var obj = {};
    selector.forEach(function (key) {
      obj[key] = ctx[key];
    });
    return obj;
  });
  var context = React.useContext(holder === null || holder === void 0 ? void 0 : holder.Context);
  var _ref2 = context || {},
    listeners = _ref2.listeners,
    getValue = _ref2.getValue;
  var valueRef = React.useRef();
  valueRef.current = eventSelector(context ? getValue() : holder === null || holder === void 0 ? void 0 : holder.defaultValue);
  var _React$useState3 = React.useState({}),
    _React$useState4 = _slicedToArray(_React$useState3, 2),
    forceUpdate = _React$useState4[1];
  useLayoutEffect(function () {
    if (!context) {
      return;
    }
    function trigger(nextValue) {
      var nextSelectorValue = eventSelector(nextValue);
      if (!isEqual(valueRef.current, nextSelectorValue, true)) {
        forceUpdate({});
      }
    }
    listeners.add(trigger);
    return function () {
      listeners.delete(trigger);
    };
  }, [context]);
  return valueRef.current;
}