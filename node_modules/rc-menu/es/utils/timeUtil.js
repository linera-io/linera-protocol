export function nextSlice(callback) {
  /* istanbul ignore next */
  Promise.resolve().then(callback);
}