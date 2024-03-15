"use strict";
"use client";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var React = _interopRequireWildcard(require("react"));
var _CaretDownFilled = _interopRequireDefault(require("@ant-design/icons/CaretDownFilled"));
var _FileOutlined = _interopRequireDefault(require("@ant-design/icons/FileOutlined"));
var _LoadingOutlined = _interopRequireDefault(require("@ant-design/icons/LoadingOutlined"));
var _MinusSquareOutlined = _interopRequireDefault(require("@ant-design/icons/MinusSquareOutlined"));
var _PlusSquareOutlined = _interopRequireDefault(require("@ant-design/icons/PlusSquareOutlined"));
var _classnames = _interopRequireDefault(require("classnames"));
var _reactNode = require("../../_util/reactNode");
const SwitcherIconCom = props => {
  const {
    prefixCls,
    switcherIcon,
    treeNodeProps,
    showLine
  } = props;
  const {
    isLeaf,
    expanded,
    loading
  } = treeNodeProps;
  if (loading) {
    return /*#__PURE__*/React.createElement(_LoadingOutlined.default, {
      className: `${prefixCls}-switcher-loading-icon`
    });
  }
  let showLeafIcon;
  if (showLine && typeof showLine === 'object') {
    showLeafIcon = showLine.showLeafIcon;
  }
  if (isLeaf) {
    if (!showLine) {
      return null;
    }
    if (typeof showLeafIcon !== 'boolean' && !!showLeafIcon) {
      const leafIcon = typeof showLeafIcon === 'function' ? showLeafIcon(treeNodeProps) : showLeafIcon;
      const leafCls = `${prefixCls}-switcher-line-custom-icon`;
      if ( /*#__PURE__*/React.isValidElement(leafIcon)) {
        return (0, _reactNode.cloneElement)(leafIcon, {
          className: (0, _classnames.default)(leafIcon.props.className || '', leafCls)
        });
      }
      return leafIcon;
    }
    return showLeafIcon ? ( /*#__PURE__*/React.createElement(_FileOutlined.default, {
      className: `${prefixCls}-switcher-line-icon`
    })) : ( /*#__PURE__*/React.createElement("span", {
      className: `${prefixCls}-switcher-leaf-line`
    }));
  }
  const switcherCls = `${prefixCls}-switcher-icon`;
  const switcher = typeof switcherIcon === 'function' ? switcherIcon(treeNodeProps) : switcherIcon;
  if ( /*#__PURE__*/React.isValidElement(switcher)) {
    return (0, _reactNode.cloneElement)(switcher, {
      className: (0, _classnames.default)(switcher.props.className || '', switcherCls)
    });
  }
  if (switcher !== undefined) {
    return switcher;
  }
  if (showLine) {
    return expanded ? ( /*#__PURE__*/React.createElement(_MinusSquareOutlined.default, {
      className: `${prefixCls}-switcher-line-icon`
    })) : ( /*#__PURE__*/React.createElement(_PlusSquareOutlined.default, {
      className: `${prefixCls}-switcher-line-icon`
    }));
  }
  return /*#__PURE__*/React.createElement(_CaretDownFilled.default, {
    className: switcherCls
  });
};
var _default = exports.default = SwitcherIconCom;