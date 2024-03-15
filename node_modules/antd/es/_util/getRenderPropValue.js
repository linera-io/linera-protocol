export const getRenderPropValue = propValue => {
  if (!propValue) {
    return null;
  }
  return typeof propValue === 'function' ? propValue() : propValue;
};