"use client";

import * as React from 'react';
import VerticalAlignTopOutlined from "@ant-design/icons/es/icons/VerticalAlignTopOutlined";
import classNames from 'classnames';
import CSSMotion from 'rc-motion';
import omit from "rc-util/es/omit";
import getScroll from '../_util/getScroll';
import { cloneElement } from '../_util/reactNode';
import scrollTo from '../_util/scrollTo';
import throttleByAnimationFrame from '../_util/throttleByAnimationFrame';
import { devUseWarning } from '../_util/warning';
import { ConfigContext } from '../config-provider';
import useStyle from './style';
const BackTop = props => {
  const {
    prefixCls: customizePrefixCls,
    className,
    rootClassName,
    visibilityHeight = 400,
    target,
    onClick,
    duration = 450
  } = props;
  const [visible, setVisible] = React.useState(visibilityHeight === 0);
  const ref = React.useRef(null);
  const getDefaultTarget = () => ref.current && ref.current.ownerDocument ? ref.current.ownerDocument : window;
  const handleScroll = throttleByAnimationFrame(e => {
    const scrollTop = getScroll(e.target, true);
    setVisible(scrollTop >= visibilityHeight);
  });
  if (process.env.NODE_ENV !== 'production') {
    const warning = devUseWarning('BackTop');
    warning.deprecated(false, 'BackTop', 'FloatButton.BackTop');
  }
  React.useEffect(() => {
    const getTarget = target || getDefaultTarget;
    const container = getTarget();
    handleScroll({
      target: container
    });
    container === null || container === void 0 ? void 0 : container.addEventListener('scroll', handleScroll);
    return () => {
      handleScroll.cancel();
      container === null || container === void 0 ? void 0 : container.removeEventListener('scroll', handleScroll);
    };
  }, [target]);
  const scrollToTop = e => {
    scrollTo(0, {
      getContainer: target || getDefaultTarget,
      duration
    });
    onClick === null || onClick === void 0 ? void 0 : onClick(e);
  };
  const {
    getPrefixCls,
    direction
  } = React.useContext(ConfigContext);
  const prefixCls = getPrefixCls('back-top', customizePrefixCls);
  const rootPrefixCls = getPrefixCls();
  const [wrapCSSVar, hashId, cssVarCls] = useStyle(prefixCls);
  const classString = classNames(hashId, cssVarCls, prefixCls, {
    [`${prefixCls}-rtl`]: direction === 'rtl'
  }, className, rootClassName);
  // fix https://fb.me/react-unknown-prop
  const divProps = omit(props, ['prefixCls', 'className', 'rootClassName', 'children', 'visibilityHeight', 'target']);
  const defaultElement = /*#__PURE__*/React.createElement("div", {
    className: `${prefixCls}-content`
  }, /*#__PURE__*/React.createElement("div", {
    className: `${prefixCls}-icon`
  }, /*#__PURE__*/React.createElement(VerticalAlignTopOutlined, null)));
  return wrapCSSVar( /*#__PURE__*/React.createElement("div", Object.assign({}, divProps, {
    className: classString,
    onClick: scrollToTop,
    ref: ref
  }), /*#__PURE__*/React.createElement(CSSMotion, {
    visible: visible,
    motionName: `${rootPrefixCls}-fade`
  }, _ref => {
    let {
      className: motionClassName
    } = _ref;
    return cloneElement(props.children || defaultElement, _ref2 => {
      let {
        className: cloneCls
      } = _ref2;
      return {
        className: classNames(motionClassName, cloneCls)
      };
    });
  })));
};
if (process.env.NODE_ENV !== 'production') {
  BackTop.displayName = 'BackTop';
}
export default BackTop;