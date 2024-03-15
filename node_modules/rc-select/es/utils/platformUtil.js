/* istanbul ignore file */
export function isPlatformMac() {
  return /(mac\sos|macintosh)/i.test(navigator.appVersion);
}