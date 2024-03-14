import * as React from 'react';
export default function useMergedConfig(propConfig, templateConfig) {
  return React.useMemo(() => {
    const support = !!propConfig;
    return [support, Object.assign(Object.assign({}, templateConfig), support && typeof propConfig === 'object' ? propConfig : null)];
  }, [propConfig]);
}