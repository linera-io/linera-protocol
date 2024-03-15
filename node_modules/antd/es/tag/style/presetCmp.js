// Style as status component
import { prepareComponentToken, prepareToken } from '.';
import { genPresetColor, genSubStyleComponent } from '../../theme/internal';
// ============================== Preset ==============================
const genPresetStyle = token => genPresetColor(token, (colorKey, _ref) => {
  let {
    textColor,
    lightBorderColor,
    lightColor,
    darkColor
  } = _ref;
  return {
    [`${token.componentCls}${token.componentCls}-${colorKey}`]: {
      color: textColor,
      background: lightColor,
      borderColor: lightBorderColor,
      // Inverse color
      '&-inverse': {
        color: token.colorTextLightSolid,
        background: darkColor,
        borderColor: darkColor
      },
      [`&${token.componentCls}-borderless`]: {
        borderColor: 'transparent'
      }
    }
  };
});
// ============================== Export ==============================
export default genSubStyleComponent(['Tag', 'preset'], token => {
  const tagToken = prepareToken(token);
  return genPresetStyle(tagToken);
}, prepareComponentToken);