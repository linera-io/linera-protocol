import { ColorFactory } from './color';
export const generateColor = color => {
  if (color instanceof ColorFactory) {
    return color;
  }
  return new ColorFactory(color);
};
export const getRoundNumber = value => Math.round(Number(value || 0));
export const getAlphaColor = color => getRoundNumber(color.toHsb().a * 100);
export const genAlphaColor = (color, alpha) => {
  const hsba = color.toHsb();
  hsba.a = alpha || 1;
  return generateColor(hsba);
};