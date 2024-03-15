import * as React from 'react';
import { ConfigContext } from '../../config-provider';
export default function useBase(customizePrefixCls, direction) {
  const {
    getPrefixCls,
    direction: rootDirection,
    renderEmpty
  } = React.useContext(ConfigContext);
  const mergedDirection = direction || rootDirection;
  const prefixCls = getPrefixCls('select', customizePrefixCls);
  const cascaderPrefixCls = getPrefixCls('cascader', customizePrefixCls);
  return [prefixCls, cascaderPrefixCls, mergedDirection, renderEmpty];
}