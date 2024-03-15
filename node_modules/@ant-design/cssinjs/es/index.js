import extractStyle from "./extractStyle";
import useCacheToken, { getComputedToken } from "./hooks/useCacheToken";
import useCSSVarRegister from "./hooks/useCSSVarRegister";
import useStyleRegister from "./hooks/useStyleRegister";
import Keyframes from "./Keyframes";
import { legacyNotSelectorLinter, logicalPropertiesLinter, NaNLinter, parentSelectorLinter } from "./linters";
import { createCache, StyleProvider } from "./StyleContext";
import { createTheme, Theme } from "./theme";
import legacyLogicalPropertiesTransformer from "./transformers/legacyLogicalProperties";
import px2remTransformer from "./transformers/px2rem";
import { supportLogicProps, supportWhere, unit } from "./util";
import { token2CSSVar } from "./util/css-variables";
export { Theme, createTheme, useStyleRegister, useCSSVarRegister, useCacheToken, createCache, StyleProvider, Keyframes, extractStyle, getComputedToken,
// Transformer
legacyLogicalPropertiesTransformer, px2remTransformer,
// Linters
logicalPropertiesLinter, legacyNotSelectorLinter, parentSelectorLinter, NaNLinter,
// util
token2CSSVar, unit };
export var _experimental = {
  supportModernCSS: function supportModernCSS() {
    return supportWhere() && supportLogicProps();
  }
};