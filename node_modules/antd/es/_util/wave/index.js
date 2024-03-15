import React, { useContext, useRef } from 'react';
import classNames from 'classnames';
import isVisible from "rc-util/es/Dom/isVisible";
import { composeRef, supportRef } from "rc-util/es/ref";
import { ConfigContext } from '../../config-provider';
import { cloneElement } from '../reactNode';
import useStyle from './style';
import useWave from './useWave';
const Wave = props => {
  const {
    children,
    disabled,
    component
  } = props;
  const {
    getPrefixCls
  } = useContext(ConfigContext);
  const containerRef = useRef(null);
  // ============================== Style ===============================
  const prefixCls = getPrefixCls('wave');
  const [, hashId] = useStyle(prefixCls);
  // =============================== Wave ===============================
  const showWave = useWave(containerRef, classNames(prefixCls, hashId), component);
  // ============================== Effect ==============================
  React.useEffect(() => {
    const node = containerRef.current;
    if (!node || node.nodeType !== 1 || disabled) {
      return;
    }
    // Click handler
    const onClick = e => {
      // Fix radio button click twice
      if (!isVisible(e.target) ||
      // No need wave
      !node.getAttribute || node.getAttribute('disabled') || node.disabled || node.className.includes('disabled') || node.className.includes('-leave')) {
        return;
      }
      showWave(e);
    };
    // Bind events
    node.addEventListener('click', onClick, true);
    return () => {
      node.removeEventListener('click', onClick, true);
    };
  }, [disabled]);
  // ============================== Render ==============================
  if (! /*#__PURE__*/React.isValidElement(children)) {
    return children !== null && children !== void 0 ? children : null;
  }
  const ref = supportRef(children) ? composeRef(children.ref, containerRef) : containerRef;
  return cloneElement(children, {
    ref
  });
};
if (process.env.NODE_ENV !== 'production') {
  Wave.displayName = 'Wave';
}
export default Wave;