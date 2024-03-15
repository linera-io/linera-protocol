import { useContext } from 'react';
import { VariantContext } from '../context';
export const Variants = ['outlined', 'borderless', 'filled'];
/**
 * Compatible for legacy `bordered` prop.
 */
const useVariant = function (variant) {
  let legacyBordered = arguments.length > 1 && arguments[1] !== undefined ? arguments[1] : undefined;
  const ctxVariant = useContext(VariantContext);
  let mergedVariant;
  if (typeof variant !== 'undefined') {
    mergedVariant = variant;
  } else if (legacyBordered === false) {
    mergedVariant = 'borderless';
  } else {
    mergedVariant = ctxVariant !== null && ctxVariant !== void 0 ? ctxVariant : 'outlined';
  }
  const enableVariantCls = Variants.includes(mergedVariant);
  return [mergedVariant, enableVariantCls];
};
export default useVariant;