import { useStyleRegister } from '@ant-design/cssinjs';
import { resetIcon } from '../../style';
import useToken from '../useToken';
const useResetIconStyle = (iconPrefixCls, csp) => {
  const [theme, token] = useToken();
  // Generate style for icons
  return useStyleRegister({
    theme,
    token,
    hashId: '',
    path: ['ant-design-icons', iconPrefixCls],
    nonce: () => csp === null || csp === void 0 ? void 0 : csp.nonce
  }, () => [{
    [`.${iconPrefixCls}`]: Object.assign(Object.assign({}, resetIcon()), {
      [`.${iconPrefixCls} .${iconPrefixCls}-icon`]: {
        display: 'block'
      }
    })
  }]);
};
export default useResetIconStyle;