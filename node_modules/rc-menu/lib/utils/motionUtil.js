"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.getMotion = getMotion;
function getMotion(mode, motion, defaultMotions) {
  if (motion) {
    return motion;
  }
  if (defaultMotions) {
    return defaultMotions[mode] || defaultMotions.other;
  }
  return undefined;
}