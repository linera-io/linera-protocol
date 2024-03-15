import _toConsumableArray from "@babel/runtime/helpers/esm/toConsumableArray";
import toArray from "rc-util/es/Children/toArray";
import * as React from 'react';
export function parseChildren(children, keyPath) {
  return toArray(children).map(function (child, index) {
    if ( /*#__PURE__*/React.isValidElement(child)) {
      var _eventKey, _child$props;
      var key = child.key;
      var eventKey = (_eventKey = (_child$props = child.props) === null || _child$props === void 0 ? void 0 : _child$props.eventKey) !== null && _eventKey !== void 0 ? _eventKey : key;
      var emptyKey = eventKey === null || eventKey === undefined;
      if (emptyKey) {
        eventKey = "tmp_key-".concat([].concat(_toConsumableArray(keyPath), [index]).join('-'));
      }
      var cloneProps = {
        key: eventKey,
        eventKey: eventKey
      };
      if (process.env.NODE_ENV !== 'production' && emptyKey) {
        cloneProps.warnKey = true;
      }
      return /*#__PURE__*/React.cloneElement(child, cloneProps);
    }
    return child;
  });
}