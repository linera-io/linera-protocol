import { resetComponent } from '../../style';
import { genCollapseMotion } from '../../style/motion';
import { genStyleHooks, mergeToken } from '../../theme/internal';
import genDraggerStyle from './dragger';
import genListStyle from './list';
import genMotionStyle from './motion';
import { genPictureCardStyle, genPictureStyle } from './picture';
import genRtlStyle from './rtl';
const genBaseStyle = token => {
  const {
    componentCls,
    colorTextDisabled
  } = token;
  return {
    [`${componentCls}-wrapper`]: Object.assign(Object.assign({}, resetComponent(token)), {
      [componentCls]: {
        outline: 0,
        "input[type='file']": {
          cursor: 'pointer'
        }
      },
      [`${componentCls}-select`]: {
        display: 'inline-block'
      },
      [`${componentCls}-disabled`]: {
        color: colorTextDisabled,
        cursor: 'not-allowed'
      }
    })
  };
};
export const prepareComponentToken = token => ({
  actionsColor: token.colorTextDescription
});
// ============================== Export ==============================
export default genStyleHooks('Upload', token => {
  const {
    fontSizeHeading3,
    fontHeight,
    lineWidth,
    controlHeightLG,
    calc
  } = token;
  const uploadToken = mergeToken(token, {
    uploadThumbnailSize: calc(fontSizeHeading3).mul(2).equal(),
    uploadProgressOffset: calc(calc(fontHeight).div(2)).add(lineWidth).equal(),
    uploadPicCardSize: calc(controlHeightLG).mul(2.55).equal()
  });
  return [genBaseStyle(uploadToken), genDraggerStyle(uploadToken), genPictureStyle(uploadToken), genPictureCardStyle(uploadToken), genListStyle(uploadToken), genMotionStyle(uploadToken), genRtlStyle(uploadToken), genCollapseMotion(uploadToken)];
}, prepareComponentToken);