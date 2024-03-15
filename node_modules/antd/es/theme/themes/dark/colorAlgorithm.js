import { TinyColor } from '@ctrl/tinycolor';
export const getAlphaColor = (baseColor, alpha) => new TinyColor(baseColor).setAlpha(alpha).toRgbString();
export const getSolidColor = (baseColor, brightness) => {
  const instance = new TinyColor(baseColor);
  return instance.lighten(brightness).toHexString();
};