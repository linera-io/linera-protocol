import canUseDom from "rc-util/es/Dom/canUseDom";
import { isStyleSupport } from "rc-util/es/Dom/styleChecker";
export const canUseDocElement = () => canUseDom() && window.document.documentElement;
export { isStyleSupport };