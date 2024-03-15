import * as React from 'react';
import { useEvent } from 'rc-util';
import raf from "rc-util/es/raf";
import showWaveEffect from './WaveEffect';
import { ConfigContext } from '../../config-provider';
import useToken from '../../theme/useToken';
import { TARGET_CLS } from './interface';
export default function useWave(nodeRef, className, component) {
  const {
    wave
  } = React.useContext(ConfigContext);
  const [, token, hashId] = useToken();
  const showWave = useEvent(event => {
    const node = nodeRef.current;
    if ((wave === null || wave === void 0 ? void 0 : wave.disabled) || !node) {
      return;
    }
    const targetNode = node.querySelector(`.${TARGET_CLS}`) || node;
    const {
      showEffect
    } = wave || {};
    // Customize wave effect
    (showEffect || showWaveEffect)(targetNode, {
      className,
      token,
      component,
      event,
      hashId
    });
  });
  const rafId = React.useRef();
  // Merge trigger event into one for each frame
  const showDebounceWave = event => {
    raf.cancel(rafId.current);
    rafId.current = raf(() => {
      showWave(event);
    });
  };
  return showDebounceWave;
}