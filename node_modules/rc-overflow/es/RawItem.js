import _extends from "@babel/runtime/helpers/esm/extends";
import _objectWithoutProperties from "@babel/runtime/helpers/esm/objectWithoutProperties";
var _excluded = ["component"],
  _excluded2 = ["className"],
  _excluded3 = ["className"];
import * as React from 'react';
import classNames from 'classnames';
import Item from './Item';
import { OverflowContext } from './context';
var InternalRawItem = function InternalRawItem(props, ref) {
  var context = React.useContext(OverflowContext);
  // Render directly when context not provided
  if (!context) {
    var _props$component = props.component,
      Component = _props$component === void 0 ? 'div' : _props$component,
      _restProps = _objectWithoutProperties(props, _excluded);
    return /*#__PURE__*/React.createElement(Component, _extends({}, _restProps, {
      ref: ref
    }));
  }
  var contextClassName = context.className,
    restContext = _objectWithoutProperties(context, _excluded2);
  var className = props.className,
    restProps = _objectWithoutProperties(props, _excluded3);
  // Do not pass context to sub item to avoid multiple measure
  return /*#__PURE__*/React.createElement(OverflowContext.Provider, {
    value: null
  }, /*#__PURE__*/React.createElement(Item, _extends({
    ref: ref,
    className: classNames(contextClassName, className)
  }, restContext, restProps)));
};
var RawItem = /*#__PURE__*/React.forwardRef(InternalRawItem);
RawItem.displayName = 'RawItem';
export default RawItem;