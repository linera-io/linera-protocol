"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.justifyContentValues = exports.flexWrapValues = exports.default = exports.alignItemsValues = void 0;
var _classnames = _interopRequireDefault(require("classnames"));
const flexWrapValues = exports.flexWrapValues = ['wrap', 'nowrap', 'wrap-reverse'];
const justifyContentValues = exports.justifyContentValues = ['flex-start', 'flex-end', 'start', 'end', 'center', 'space-between', 'space-around', 'space-evenly', 'stretch', 'normal', 'left', 'right'];
const alignItemsValues = exports.alignItemsValues = ['center', 'start', 'end', 'flex-start', 'flex-end', 'self-start', 'self-end', 'baseline', 'normal', 'stretch'];
const genClsWrap = (prefixCls, props) => {
  const wrapCls = {};
  flexWrapValues.forEach(cssKey => {
    wrapCls[`${prefixCls}-wrap-${cssKey}`] = props.wrap === cssKey;
  });
  return wrapCls;
};
const genClsAlign = (prefixCls, props) => {
  const alignCls = {};
  alignItemsValues.forEach(cssKey => {
    alignCls[`${prefixCls}-align-${cssKey}`] = props.align === cssKey;
  });
  alignCls[`${prefixCls}-align-stretch`] = !props.align && !!props.vertical;
  return alignCls;
};
const genClsJustify = (prefixCls, props) => {
  const justifyCls = {};
  justifyContentValues.forEach(cssKey => {
    justifyCls[`${prefixCls}-justify-${cssKey}`] = props.justify === cssKey;
  });
  return justifyCls;
};
function createFlexClassNames(prefixCls, props) {
  return (0, _classnames.default)(Object.assign(Object.assign(Object.assign({}, genClsWrap(prefixCls, props)), genClsAlign(prefixCls, props)), genClsJustify(prefixCls, props)));
}
var _default = exports.default = createFlexClassNames;