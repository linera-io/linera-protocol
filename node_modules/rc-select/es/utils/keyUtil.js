import KeyCode from "rc-util/es/KeyCode";

/** keyCode Judgment function */
export function isValidateOpenKey(currentKeyCode) {
  return ![
  // System function button
  KeyCode.ESC, KeyCode.SHIFT, KeyCode.BACKSPACE, KeyCode.TAB, KeyCode.WIN_KEY, KeyCode.ALT, KeyCode.META, KeyCode.WIN_KEY_RIGHT, KeyCode.CTRL, KeyCode.SEMICOLON, KeyCode.EQUALS, KeyCode.CAPS_LOCK, KeyCode.CONTEXT_MENU,
  // F1-F12
  KeyCode.F1, KeyCode.F2, KeyCode.F3, KeyCode.F4, KeyCode.F5, KeyCode.F6, KeyCode.F7, KeyCode.F8, KeyCode.F9, KeyCode.F10, KeyCode.F11, KeyCode.F12].includes(currentKeyCode);
}