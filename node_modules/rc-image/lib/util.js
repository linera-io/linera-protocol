"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.isImageValid = isImageValid;
function isImageValid(src) {
  return new Promise(function (resolve) {
    var img = document.createElement('img');
    img.onerror = function () {
      return resolve(false);
    };
    img.onload = function () {
      return resolve(true);
    };
    img.src = src;
  });
}