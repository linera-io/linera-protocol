"use client";

import * as React from 'react';
import CaretDownFilled from "@ant-design/icons/es/icons/CaretDownFilled";
import FileOutlined from "@ant-design/icons/es/icons/FileOutlined";
import LoadingOutlined from "@ant-design/icons/es/icons/LoadingOutlined";
import MinusSquareOutlined from "@ant-design/icons/es/icons/MinusSquareOutlined";
import PlusSquareOutlined from "@ant-design/icons/es/icons/PlusSquareOutlined";
import classNames from 'classnames';
import { cloneElement } from '../../_util/reactNode';
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
    return /*#__PURE__*/React.createElement(LoadingOutlined, {
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
        return cloneElement(leafIcon, {
          className: classNames(leafIcon.props.className || '', leafCls)
        });
      }
      return leafIcon;
    }
    return showLeafIcon ? ( /*#__PURE__*/React.createElement(FileOutlined, {
      className: `${prefixCls}-switcher-line-icon`
    })) : ( /*#__PURE__*/React.createElement("span", {
      className: `${prefixCls}-switcher-leaf-line`
    }));
  }
  const switcherCls = `${prefixCls}-switcher-icon`;
  const switcher = typeof switcherIcon === 'function' ? switcherIcon(treeNodeProps) : switcherIcon;
  if ( /*#__PURE__*/React.isValidElement(switcher)) {
    return cloneElement(switcher, {
      className: classNames(switcher.props.className || '', switcherCls)
    });
  }
  if (switcher !== undefined) {
    return switcher;
  }
  if (showLine) {
    return expanded ? ( /*#__PURE__*/React.createElement(MinusSquareOutlined, {
      className: `${prefixCls}-switcher-line-icon`
    })) : ( /*#__PURE__*/React.createElement(PlusSquareOutlined, {
      className: `${prefixCls}-switcher-line-icon`
    }));
  }
  return /*#__PURE__*/React.createElement(CaretDownFilled, {
    className: switcherCls
  });
};
export default SwitcherIconCom;