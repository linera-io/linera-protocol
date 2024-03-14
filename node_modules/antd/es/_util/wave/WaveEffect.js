"use client";

import classNames from 'classnames';
import CSSMotion from 'rc-motion';
import { render, unmount } from "rc-util/es/React/render";
import raf from "rc-util/es/raf";
import * as React from 'react';
import { getTargetWaveColor } from './util';
import { TARGET_CLS } from './interface';
function validateNum(value) {
  return Number.isNaN(value) ? 0 : value;
}
const WaveEffect = props => {
  const {
    className,
    target,
    component
  } = props;
  const divRef = React.useRef(null);
  const [color, setWaveColor] = React.useState(null);
  const [borderRadius, setBorderRadius] = React.useState([]);
  const [left, setLeft] = React.useState(0);
  const [top, setTop] = React.useState(0);
  const [width, setWidth] = React.useState(0);
  const [height, setHeight] = React.useState(0);
  const [enabled, setEnabled] = React.useState(false);
  const waveStyle = {
    left,
    top,
    width,
    height,
    borderRadius: borderRadius.map(radius => `${radius}px`).join(' ')
  };
  if (color) {
    waveStyle['--wave-color'] = color;
  }
  function syncPos() {
    const nodeStyle = getComputedStyle(target);
    // Get wave color from target
    setWaveColor(getTargetWaveColor(target));
    const isStatic = nodeStyle.position === 'static';
    // Rect
    const {
      borderLeftWidth,
      borderTopWidth
    } = nodeStyle;
    setLeft(isStatic ? target.offsetLeft : validateNum(-parseFloat(borderLeftWidth)));
    setTop(isStatic ? target.offsetTop : validateNum(-parseFloat(borderTopWidth)));
    setWidth(target.offsetWidth);
    setHeight(target.offsetHeight);
    // Get border radius
    const {
      borderTopLeftRadius,
      borderTopRightRadius,
      borderBottomLeftRadius,
      borderBottomRightRadius
    } = nodeStyle;
    setBorderRadius([borderTopLeftRadius, borderTopRightRadius, borderBottomRightRadius, borderBottomLeftRadius].map(radius => validateNum(parseFloat(radius))));
  }
  React.useEffect(() => {
    if (target) {
      // We need delay to check position here
      // since UI may change after click
      const id = raf(() => {
        syncPos();
        setEnabled(true);
      });
      // Add resize observer to follow size
      let resizeObserver;
      if (typeof ResizeObserver !== 'undefined') {
        resizeObserver = new ResizeObserver(syncPos);
        resizeObserver.observe(target);
      }
      return () => {
        raf.cancel(id);
        resizeObserver === null || resizeObserver === void 0 ? void 0 : resizeObserver.disconnect();
      };
    }
  }, []);
  if (!enabled) {
    return null;
  }
  const isSmallComponent = (component === 'Checkbox' || component === 'Radio') && (target === null || target === void 0 ? void 0 : target.classList.contains(TARGET_CLS));
  return /*#__PURE__*/React.createElement(CSSMotion, {
    visible: true,
    motionAppear: true,
    motionName: "wave-motion",
    motionDeadline: 5000,
    onAppearEnd: (_, event) => {
      var _a;
      if (event.deadline || event.propertyName === 'opacity') {
        const holder = (_a = divRef.current) === null || _a === void 0 ? void 0 : _a.parentElement;
        unmount(holder).then(() => {
          holder === null || holder === void 0 ? void 0 : holder.remove();
        });
      }
      return false;
    }
  }, _ref => {
    let {
      className: motionClassName
    } = _ref;
    return /*#__PURE__*/React.createElement("div", {
      ref: divRef,
      className: classNames(className, {
        'wave-quick': isSmallComponent
      }, motionClassName),
      style: waveStyle
    });
  });
};
const showWaveEffect = (target, info) => {
  var _a;
  const {
    component
  } = info;
  // Skip for unchecked checkbox
  if (component === 'Checkbox' && !((_a = target.querySelector('input')) === null || _a === void 0 ? void 0 : _a.checked)) {
    return;
  }
  // Create holder
  const holder = document.createElement('div');
  holder.style.position = 'absolute';
  holder.style.left = '0px';
  holder.style.top = '0px';
  target === null || target === void 0 ? void 0 : target.insertBefore(holder, target === null || target === void 0 ? void 0 : target.firstChild);
  render( /*#__PURE__*/React.createElement(WaveEffect, Object.assign({}, info, {
    target: target
  })), holder);
};
export default showWaveEffect;