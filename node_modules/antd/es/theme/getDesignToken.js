import { createTheme, getComputedToken } from '@ant-design/cssinjs';
import defaultDerivative from './themes/default';
import seedToken from './themes/seed';
import formatToken from './util/alias';
const getDesignToken = config => {
  const theme = (config === null || config === void 0 ? void 0 : config.algorithm) ? createTheme(config.algorithm) : createTheme(defaultDerivative);
  const mergedToken = Object.assign(Object.assign({}, seedToken), config === null || config === void 0 ? void 0 : config.token);
  return getComputedToken(mergedToken, {
    override: config === null || config === void 0 ? void 0 : config.token
  }, theme, formatToken);
};
export default getDesignToken;