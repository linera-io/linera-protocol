"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.prepareComponentToken = exports.default = void 0;
var _internal = require("../../theme/internal");
var _utils = require("../utils");
const genFlexStyle = token => {
  const {
    componentCls
  } = token;
  return {
    [componentCls]: {
      display: 'flex',
      '&-vertical': {
        flexDirection: 'column'
      },
      '&-rtl': {
        direction: 'rtl'
      },
      '&:empty': {
        display: 'none'
      }
    }
  };
};
const genFlexGapStyle = token => {
  const {
    componentCls
  } = token;
  return {
    [componentCls]: {
      '&-gap-small': {
        gap: token.flexGapSM
      },
      '&-gap-middle': {
        gap: token.flexGap
      },
      '&-gap-large': {
        gap: token.flexGapLG
      }
    }
  };
};
const genFlexWrapStyle = token => {
  const {
    componentCls
  } = token;
  const wrapStyle = {};
  _utils.flexWrapValues.forEach(value => {
    wrapStyle[`${componentCls}-wrap-${value}`] = {
      flexWrap: value
    };
  });
  return wrapStyle;
};
const genAlignItemsStyle = token => {
  const {
    componentCls
  } = token;
  const alignStyle = {};
  _utils.alignItemsValues.forEach(value => {
    alignStyle[`${componentCls}-align-${value}`] = {
      alignItems: value
    };
  });
  return alignStyle;
};
const genJustifyContentStyle = token => {
  const {
    componentCls
  } = token;
  const justifyStyle = {};
  _utils.justifyContentValues.forEach(value => {
    justifyStyle[`${componentCls}-justify-${value}`] = {
      justifyContent: value
    };
  });
  return justifyStyle;
};
const prepareComponentToken = () => ({});
exports.prepareComponentToken = prepareComponentToken;
var _default = exports.default = (0, _internal.genStyleHooks)('Flex', token => {
  const {
    paddingXS,
    padding,
    paddingLG
  } = token;
  const flexToken = (0, _internal.mergeToken)(token, {
    flexGapSM: paddingXS,
    flexGap: padding,
    flexGapLG: paddingLG
  });
  return [genFlexStyle(flexToken), genFlexGapStyle(flexToken), genFlexWrapStyle(flexToken), genAlignItemsStyle(flexToken), genJustifyContentStyle(flexToken)];
}, prepareComponentToken, {
  // Flex component don't apply extra font style
  // https://github.com/ant-design/ant-design/issues/46403
  resetStyle: false
});