"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.prepareComponentToken = exports.default = void 0;
var _cssinjs = require("@ant-design/cssinjs");
var _style = require("../../style");
var _internal = require("../../theme/internal");
var _motion = _interopRequireDefault(require("./motion"));
const genCardStyle = token => {
  const {
    componentCls,
    tabsCardPadding,
    cardBg,
    cardGutter,
    colorBorderSecondary,
    itemSelectedColor
  } = token;
  return {
    [`${componentCls}-card`]: {
      [`> ${componentCls}-nav, > div > ${componentCls}-nav`]: {
        [`${componentCls}-tab`]: {
          margin: 0,
          padding: tabsCardPadding,
          background: cardBg,
          border: `${(0, _cssinjs.unit)(token.lineWidth)} ${token.lineType} ${colorBorderSecondary}`,
          transition: `all ${token.motionDurationSlow} ${token.motionEaseInOut}`
        },
        [`${componentCls}-tab-active`]: {
          color: itemSelectedColor,
          background: token.colorBgContainer
        },
        [`${componentCls}-ink-bar`]: {
          visibility: 'hidden'
        }
      },
      // ========================== Top & Bottom ==========================
      [`&${componentCls}-top, &${componentCls}-bottom`]: {
        [`> ${componentCls}-nav, > div > ${componentCls}-nav`]: {
          [`${componentCls}-tab + ${componentCls}-tab`]: {
            marginLeft: {
              _skip_check_: true,
              value: (0, _cssinjs.unit)(cardGutter)
            }
          }
        }
      },
      [`&${componentCls}-top`]: {
        [`> ${componentCls}-nav, > div > ${componentCls}-nav`]: {
          [`${componentCls}-tab`]: {
            borderRadius: `${(0, _cssinjs.unit)(token.borderRadiusLG)} ${(0, _cssinjs.unit)(token.borderRadiusLG)} 0 0`
          },
          [`${componentCls}-tab-active`]: {
            borderBottomColor: token.colorBgContainer
          }
        }
      },
      [`&${componentCls}-bottom`]: {
        [`> ${componentCls}-nav, > div > ${componentCls}-nav`]: {
          [`${componentCls}-tab`]: {
            borderRadius: `0 0 ${(0, _cssinjs.unit)(token.borderRadiusLG)} ${(0, _cssinjs.unit)(token.borderRadiusLG)}`
          },
          [`${componentCls}-tab-active`]: {
            borderTopColor: token.colorBgContainer
          }
        }
      },
      // ========================== Left & Right ==========================
      [`&${componentCls}-left, &${componentCls}-right`]: {
        [`> ${componentCls}-nav, > div > ${componentCls}-nav`]: {
          [`${componentCls}-tab + ${componentCls}-tab`]: {
            marginTop: (0, _cssinjs.unit)(cardGutter)
          }
        }
      },
      [`&${componentCls}-left`]: {
        [`> ${componentCls}-nav, > div > ${componentCls}-nav`]: {
          [`${componentCls}-tab`]: {
            borderRadius: {
              _skip_check_: true,
              value: `${(0, _cssinjs.unit)(token.borderRadiusLG)} 0 0 ${(0, _cssinjs.unit)(token.borderRadiusLG)}`
            }
          },
          [`${componentCls}-tab-active`]: {
            borderRightColor: {
              _skip_check_: true,
              value: token.colorBgContainer
            }
          }
        }
      },
      [`&${componentCls}-right`]: {
        [`> ${componentCls}-nav, > div > ${componentCls}-nav`]: {
          [`${componentCls}-tab`]: {
            borderRadius: {
              _skip_check_: true,
              value: `0 ${(0, _cssinjs.unit)(token.borderRadiusLG)} ${(0, _cssinjs.unit)(token.borderRadiusLG)} 0`
            }
          },
          [`${componentCls}-tab-active`]: {
            borderLeftColor: {
              _skip_check_: true,
              value: token.colorBgContainer
            }
          }
        }
      }
    }
  };
};
const genDropdownStyle = token => {
  const {
    componentCls,
    itemHoverColor,
    dropdownEdgeChildVerticalPadding
  } = token;
  return {
    [`${componentCls}-dropdown`]: Object.assign(Object.assign({}, (0, _style.resetComponent)(token)), {
      position: 'absolute',
      top: -9999,
      left: {
        _skip_check_: true,
        value: -9999
      },
      zIndex: token.zIndexPopup,
      display: 'block',
      '&-hidden': {
        display: 'none'
      },
      [`${componentCls}-dropdown-menu`]: {
        maxHeight: token.tabsDropdownHeight,
        margin: 0,
        padding: `${(0, _cssinjs.unit)(dropdownEdgeChildVerticalPadding)} 0`,
        overflowX: 'hidden',
        overflowY: 'auto',
        textAlign: {
          _skip_check_: true,
          value: 'left'
        },
        listStyleType: 'none',
        backgroundColor: token.colorBgContainer,
        backgroundClip: 'padding-box',
        borderRadius: token.borderRadiusLG,
        outline: 'none',
        boxShadow: token.boxShadowSecondary,
        '&-item': Object.assign(Object.assign({}, _style.textEllipsis), {
          display: 'flex',
          alignItems: 'center',
          minWidth: token.tabsDropdownWidth,
          margin: 0,
          padding: `${(0, _cssinjs.unit)(token.paddingXXS)} ${(0, _cssinjs.unit)(token.paddingSM)}`,
          color: token.colorText,
          fontWeight: 'normal',
          fontSize: token.fontSize,
          lineHeight: token.lineHeight,
          cursor: 'pointer',
          transition: `all ${token.motionDurationSlow}`,
          '> span': {
            flex: 1,
            whiteSpace: 'nowrap'
          },
          '&-remove': {
            flex: 'none',
            marginLeft: {
              _skip_check_: true,
              value: token.marginSM
            },
            color: token.colorTextDescription,
            fontSize: token.fontSizeSM,
            background: 'transparent',
            border: 0,
            cursor: 'pointer',
            '&:hover': {
              color: itemHoverColor
            }
          },
          '&:hover': {
            background: token.controlItemBgHover
          },
          '&-disabled': {
            '&, &:hover': {
              color: token.colorTextDisabled,
              background: 'transparent',
              cursor: 'not-allowed'
            }
          }
        })
      }
    })
  };
};
const genPositionStyle = token => {
  const {
    componentCls,
    margin,
    colorBorderSecondary,
    horizontalMargin,
    verticalItemPadding,
    verticalItemMargin,
    calc
  } = token;
  return {
    // ========================== Top & Bottom ==========================
    [`${componentCls}-top, ${componentCls}-bottom`]: {
      flexDirection: 'column',
      [`> ${componentCls}-nav, > div > ${componentCls}-nav`]: {
        margin: horizontalMargin,
        '&::before': {
          position: 'absolute',
          right: {
            _skip_check_: true,
            value: 0
          },
          left: {
            _skip_check_: true,
            value: 0
          },
          borderBottom: `${(0, _cssinjs.unit)(token.lineWidth)} ${token.lineType} ${colorBorderSecondary}`,
          content: "''"
        },
        [`${componentCls}-ink-bar`]: {
          height: token.lineWidthBold,
          '&-animated': {
            transition: `width ${token.motionDurationSlow}, left ${token.motionDurationSlow},
            right ${token.motionDurationSlow}`
          }
        },
        [`${componentCls}-nav-wrap`]: {
          '&::before, &::after': {
            top: 0,
            bottom: 0,
            width: token.controlHeight
          },
          '&::before': {
            left: {
              _skip_check_: true,
              value: 0
            },
            boxShadow: token.boxShadowTabsOverflowLeft
          },
          '&::after': {
            right: {
              _skip_check_: true,
              value: 0
            },
            boxShadow: token.boxShadowTabsOverflowRight
          },
          [`&${componentCls}-nav-wrap-ping-left::before`]: {
            opacity: 1
          },
          [`&${componentCls}-nav-wrap-ping-right::after`]: {
            opacity: 1
          }
        }
      }
    },
    [`${componentCls}-top`]: {
      [`> ${componentCls}-nav,
        > div > ${componentCls}-nav`]: {
        '&::before': {
          bottom: 0
        },
        [`${componentCls}-ink-bar`]: {
          bottom: 0
        }
      }
    },
    [`${componentCls}-bottom`]: {
      [`> ${componentCls}-nav, > div > ${componentCls}-nav`]: {
        order: 1,
        marginTop: margin,
        marginBottom: 0,
        '&::before': {
          top: 0
        },
        [`${componentCls}-ink-bar`]: {
          top: 0
        }
      },
      [`> ${componentCls}-content-holder, > div > ${componentCls}-content-holder`]: {
        order: 0
      }
    },
    // ========================== Left & Right ==========================
    [`${componentCls}-left, ${componentCls}-right`]: {
      [`> ${componentCls}-nav, > div > ${componentCls}-nav`]: {
        flexDirection: 'column',
        minWidth: calc(token.controlHeight).mul(1.25).equal(),
        // >>>>>>>>>>> Tab
        [`${componentCls}-tab`]: {
          padding: verticalItemPadding,
          textAlign: 'center'
        },
        [`${componentCls}-tab + ${componentCls}-tab`]: {
          margin: verticalItemMargin
        },
        // >>>>>>>>>>> Nav
        [`${componentCls}-nav-wrap`]: {
          flexDirection: 'column',
          '&::before, &::after': {
            right: {
              _skip_check_: true,
              value: 0
            },
            left: {
              _skip_check_: true,
              value: 0
            },
            height: token.controlHeight
          },
          '&::before': {
            top: 0,
            boxShadow: token.boxShadowTabsOverflowTop
          },
          '&::after': {
            bottom: 0,
            boxShadow: token.boxShadowTabsOverflowBottom
          },
          [`&${componentCls}-nav-wrap-ping-top::before`]: {
            opacity: 1
          },
          [`&${componentCls}-nav-wrap-ping-bottom::after`]: {
            opacity: 1
          }
        },
        // >>>>>>>>>>> Ink Bar
        [`${componentCls}-ink-bar`]: {
          width: token.lineWidthBold,
          '&-animated': {
            transition: `height ${token.motionDurationSlow}, top ${token.motionDurationSlow}`
          }
        },
        [`${componentCls}-nav-list, ${componentCls}-nav-operations`]: {
          flex: '1 0 auto',
          // fix safari scroll problem
          flexDirection: 'column'
        }
      }
    },
    [`${componentCls}-left`]: {
      [`> ${componentCls}-nav, > div > ${componentCls}-nav`]: {
        [`${componentCls}-ink-bar`]: {
          right: {
            _skip_check_: true,
            value: 0
          }
        }
      },
      [`> ${componentCls}-content-holder, > div > ${componentCls}-content-holder`]: {
        marginLeft: {
          _skip_check_: true,
          value: (0, _cssinjs.unit)(calc(token.lineWidth).mul(-1).equal())
        },
        borderLeft: {
          _skip_check_: true,
          value: `${(0, _cssinjs.unit)(token.lineWidth)} ${token.lineType} ${token.colorBorder}`
        },
        [`> ${componentCls}-content > ${componentCls}-tabpane`]: {
          paddingLeft: {
            _skip_check_: true,
            value: token.paddingLG
          }
        }
      }
    },
    [`${componentCls}-right`]: {
      [`> ${componentCls}-nav, > div > ${componentCls}-nav`]: {
        order: 1,
        [`${componentCls}-ink-bar`]: {
          left: {
            _skip_check_: true,
            value: 0
          }
        }
      },
      [`> ${componentCls}-content-holder, > div > ${componentCls}-content-holder`]: {
        order: 0,
        marginRight: {
          _skip_check_: true,
          value: calc(token.lineWidth).mul(-1).equal()
        },
        borderRight: {
          _skip_check_: true,
          value: `${(0, _cssinjs.unit)(token.lineWidth)} ${token.lineType} ${token.colorBorder}`
        },
        [`> ${componentCls}-content > ${componentCls}-tabpane`]: {
          paddingRight: {
            _skip_check_: true,
            value: token.paddingLG
          }
        }
      }
    }
  };
};
const genSizeStyle = token => {
  const {
    componentCls,
    cardPaddingSM,
    cardPaddingLG,
    horizontalItemPaddingSM,
    horizontalItemPaddingLG
  } = token;
  return {
    [componentCls]: {
      '&-small': {
        [`> ${componentCls}-nav`]: {
          [`${componentCls}-tab`]: {
            padding: horizontalItemPaddingSM,
            fontSize: token.titleFontSizeSM
          }
        }
      },
      '&-large': {
        [`> ${componentCls}-nav`]: {
          [`${componentCls}-tab`]: {
            padding: horizontalItemPaddingLG,
            fontSize: token.titleFontSizeLG
          }
        }
      }
    },
    [`${componentCls}-card`]: {
      [`&${componentCls}-small`]: {
        [`> ${componentCls}-nav`]: {
          [`${componentCls}-tab`]: {
            padding: cardPaddingSM
          }
        },
        [`&${componentCls}-bottom`]: {
          [`> ${componentCls}-nav ${componentCls}-tab`]: {
            borderRadius: `0 0 ${(0, _cssinjs.unit)(token.borderRadius)} ${(0, _cssinjs.unit)(token.borderRadius)}`
          }
        },
        [`&${componentCls}-top`]: {
          [`> ${componentCls}-nav ${componentCls}-tab`]: {
            borderRadius: `${(0, _cssinjs.unit)(token.borderRadius)} ${(0, _cssinjs.unit)(token.borderRadius)} 0 0`
          }
        },
        [`&${componentCls}-right`]: {
          [`> ${componentCls}-nav ${componentCls}-tab`]: {
            borderRadius: {
              _skip_check_: true,
              value: `0 ${(0, _cssinjs.unit)(token.borderRadius)} ${(0, _cssinjs.unit)(token.borderRadius)} 0`
            }
          }
        },
        [`&${componentCls}-left`]: {
          [`> ${componentCls}-nav ${componentCls}-tab`]: {
            borderRadius: {
              _skip_check_: true,
              value: `${(0, _cssinjs.unit)(token.borderRadius)} 0 0 ${(0, _cssinjs.unit)(token.borderRadius)}`
            }
          }
        }
      },
      [`&${componentCls}-large`]: {
        [`> ${componentCls}-nav`]: {
          [`${componentCls}-tab`]: {
            padding: cardPaddingLG
          }
        }
      }
    }
  };
};
const genTabStyle = token => {
  const {
    componentCls,
    itemActiveColor,
    itemHoverColor,
    iconCls,
    tabsHorizontalItemMargin,
    horizontalItemPadding,
    itemSelectedColor,
    itemColor
  } = token;
  const tabCls = `${componentCls}-tab`;
  return {
    [tabCls]: {
      position: 'relative',
      WebkitTouchCallout: 'none',
      WebkitTapHighlightColor: 'transparent',
      display: 'inline-flex',
      alignItems: 'center',
      padding: horizontalItemPadding,
      fontSize: token.titleFontSize,
      background: 'transparent',
      border: 0,
      outline: 'none',
      cursor: 'pointer',
      color: itemColor,
      '&-btn, &-remove': Object.assign({
        '&:focus:not(:focus-visible), &:active': {
          color: itemActiveColor
        }
      }, (0, _style.genFocusStyle)(token)),
      '&-btn': {
        outline: 'none',
        transition: 'all 0.3s',
        [`${tabCls}-icon:not(:last-child)`]: {
          marginInlineEnd: token.marginSM
        }
      },
      '&-remove': {
        flex: 'none',
        marginRight: {
          _skip_check_: true,
          value: token.calc(token.marginXXS).mul(-1).equal()
        },
        marginLeft: {
          _skip_check_: true,
          value: token.marginXS
        },
        color: token.colorTextDescription,
        fontSize: token.fontSizeSM,
        background: 'transparent',
        border: 'none',
        outline: 'none',
        cursor: 'pointer',
        transition: `all ${token.motionDurationSlow}`,
        '&:hover': {
          color: token.colorTextHeading
        }
      },
      '&:hover': {
        color: itemHoverColor
      },
      [`&${tabCls}-active ${tabCls}-btn`]: {
        color: itemSelectedColor,
        textShadow: token.tabsActiveTextShadow
      },
      [`&${tabCls}-disabled`]: {
        color: token.colorTextDisabled,
        cursor: 'not-allowed'
      },
      [`&${tabCls}-disabled ${tabCls}-btn, &${tabCls}-disabled ${componentCls}-remove`]: {
        '&:focus, &:active': {
          color: token.colorTextDisabled
        }
      },
      [`& ${tabCls}-remove ${iconCls}`]: {
        margin: 0
      },
      [`${iconCls}:not(:last-child)`]: {
        marginRight: {
          _skip_check_: true,
          value: token.marginSM
        }
      }
    },
    [`${tabCls} + ${tabCls}`]: {
      margin: {
        _skip_check_: true,
        value: tabsHorizontalItemMargin
      }
    }
  };
};
const genRtlStyle = token => {
  const {
    componentCls,
    tabsHorizontalItemMarginRTL,
    iconCls,
    cardGutter,
    calc
  } = token;
  const rtlCls = `${componentCls}-rtl`;
  return {
    [rtlCls]: {
      direction: 'rtl',
      [`${componentCls}-nav`]: {
        [`${componentCls}-tab`]: {
          margin: {
            _skip_check_: true,
            value: tabsHorizontalItemMarginRTL
          },
          [`${componentCls}-tab:last-of-type`]: {
            marginLeft: {
              _skip_check_: true,
              value: 0
            }
          },
          [iconCls]: {
            marginRight: {
              _skip_check_: true,
              value: 0
            },
            marginLeft: {
              _skip_check_: true,
              value: (0, _cssinjs.unit)(token.marginSM)
            }
          },
          [`${componentCls}-tab-remove`]: {
            marginRight: {
              _skip_check_: true,
              value: (0, _cssinjs.unit)(token.marginXS)
            },
            marginLeft: {
              _skip_check_: true,
              value: (0, _cssinjs.unit)(calc(token.marginXXS).mul(-1).equal())
            },
            [iconCls]: {
              margin: 0
            }
          }
        }
      },
      [`&${componentCls}-left`]: {
        [`> ${componentCls}-nav`]: {
          order: 1
        },
        [`> ${componentCls}-content-holder`]: {
          order: 0
        }
      },
      [`&${componentCls}-right`]: {
        [`> ${componentCls}-nav`]: {
          order: 0
        },
        [`> ${componentCls}-content-holder`]: {
          order: 1
        }
      },
      // ====================== Card ======================
      [`&${componentCls}-card${componentCls}-top, &${componentCls}-card${componentCls}-bottom`]: {
        [`> ${componentCls}-nav, > div > ${componentCls}-nav`]: {
          [`${componentCls}-tab + ${componentCls}-tab`]: {
            marginRight: {
              _skip_check_: true,
              value: cardGutter
            },
            marginLeft: {
              _skip_check_: true,
              value: 0
            }
          }
        }
      }
    },
    [`${componentCls}-dropdown-rtl`]: {
      direction: 'rtl'
    },
    [`${componentCls}-menu-item`]: {
      [`${componentCls}-dropdown-rtl`]: {
        textAlign: {
          _skip_check_: true,
          value: 'right'
        }
      }
    }
  };
};
const genTabsStyle = token => {
  const {
    componentCls,
    tabsCardPadding,
    cardHeight,
    cardGutter,
    itemHoverColor,
    itemActiveColor,
    colorBorderSecondary
  } = token;
  return {
    [componentCls]: Object.assign(Object.assign(Object.assign(Object.assign({}, (0, _style.resetComponent)(token)), {
      display: 'flex',
      // ========================== Navigation ==========================
      [`> ${componentCls}-nav, > div > ${componentCls}-nav`]: {
        position: 'relative',
        display: 'flex',
        flex: 'none',
        alignItems: 'center',
        [`${componentCls}-nav-wrap`]: {
          position: 'relative',
          display: 'flex',
          flex: 'auto',
          alignSelf: 'stretch',
          overflow: 'hidden',
          whiteSpace: 'nowrap',
          transform: 'translate(0)',
          // Fix chrome render bug
          // >>>>> Ping shadow
          '&::before, &::after': {
            position: 'absolute',
            zIndex: 1,
            opacity: 0,
            transition: `opacity ${token.motionDurationSlow}`,
            content: "''",
            pointerEvents: 'none'
          }
        },
        [`${componentCls}-nav-list`]: {
          position: 'relative',
          display: 'flex',
          transition: `opacity ${token.motionDurationSlow}`
        },
        // >>>>>>>> Operations
        [`${componentCls}-nav-operations`]: {
          display: 'flex',
          alignSelf: 'stretch'
        },
        [`${componentCls}-nav-operations-hidden`]: {
          position: 'absolute',
          visibility: 'hidden',
          pointerEvents: 'none'
        },
        [`${componentCls}-nav-more`]: {
          position: 'relative',
          padding: tabsCardPadding,
          background: 'transparent',
          border: 0,
          color: token.colorText,
          '&::after': {
            position: 'absolute',
            right: {
              _skip_check_: true,
              value: 0
            },
            bottom: 0,
            left: {
              _skip_check_: true,
              value: 0
            },
            height: token.calc(token.controlHeightLG).div(8).equal(),
            transform: 'translateY(100%)',
            content: "''"
          }
        },
        [`${componentCls}-nav-add`]: Object.assign({
          minWidth: cardHeight,
          minHeight: cardHeight,
          marginLeft: {
            _skip_check_: true,
            value: cardGutter
          },
          padding: `0 ${(0, _cssinjs.unit)(token.paddingXS)}`,
          background: 'transparent',
          border: `${(0, _cssinjs.unit)(token.lineWidth)} ${token.lineType} ${colorBorderSecondary}`,
          borderRadius: `${(0, _cssinjs.unit)(token.borderRadiusLG)} ${(0, _cssinjs.unit)(token.borderRadiusLG)} 0 0`,
          outline: 'none',
          cursor: 'pointer',
          color: token.colorText,
          transition: `all ${token.motionDurationSlow} ${token.motionEaseInOut}`,
          '&:hover': {
            color: itemHoverColor
          },
          '&:active, &:focus:not(:focus-visible)': {
            color: itemActiveColor
          }
        }, (0, _style.genFocusStyle)(token))
      },
      [`${componentCls}-extra-content`]: {
        flex: 'none'
      },
      // ============================ InkBar ============================
      [`${componentCls}-ink-bar`]: {
        position: 'absolute',
        background: token.inkBarColor,
        pointerEvents: 'none'
      }
    }), genTabStyle(token)), {
      // =========================== TabPanes ===========================
      [`${componentCls}-content`]: {
        position: 'relative',
        width: '100%'
      },
      [`${componentCls}-content-holder`]: {
        flex: 'auto',
        minWidth: 0,
        minHeight: 0
      },
      [`${componentCls}-tabpane`]: {
        outline: 'none',
        '&-hidden': {
          display: 'none'
        }
      }
    }),
    [`${componentCls}-centered`]: {
      [`> ${componentCls}-nav, > div > ${componentCls}-nav`]: {
        [`${componentCls}-nav-wrap`]: {
          [`&:not([class*='${componentCls}-nav-wrap-ping'])`]: {
            justifyContent: 'center'
          }
        }
      }
    }
  };
};
const prepareComponentToken = token => {
  const cardHeight = token.controlHeightLG;
  return {
    zIndexPopup: token.zIndexPopupBase + 50,
    cardBg: token.colorFillAlter,
    cardHeight,
    // Initialize with empty string, because cardPadding will be calculated with cardHeight by default.
    cardPadding: `${(cardHeight - Math.round(token.fontSize * token.lineHeight)) / 2 - token.lineWidth}px ${token.padding}px`,
    cardPaddingSM: `${token.paddingXXS * 1.5}px ${token.padding}px`,
    cardPaddingLG: `${token.paddingXS}px ${token.padding}px ${token.paddingXXS * 1.5}px`,
    titleFontSize: token.fontSize,
    titleFontSizeLG: token.fontSizeLG,
    titleFontSizeSM: token.fontSize,
    inkBarColor: token.colorPrimary,
    horizontalMargin: `0 0 ${token.margin}px 0`,
    horizontalItemGutter: 32,
    // Fixed Value
    // Initialize with empty string, because horizontalItemMargin will be calculated with horizontalItemGutter by default.
    horizontalItemMargin: ``,
    horizontalItemMarginRTL: ``,
    horizontalItemPadding: `${token.paddingSM}px 0`,
    horizontalItemPaddingSM: `${token.paddingXS}px 0`,
    horizontalItemPaddingLG: `${token.padding}px 0`,
    verticalItemPadding: `${token.paddingXS}px ${token.paddingLG}px`,
    verticalItemMargin: `${token.margin}px 0 0 0`,
    itemColor: token.colorText,
    itemSelectedColor: token.colorPrimary,
    itemHoverColor: token.colorPrimaryHover,
    itemActiveColor: token.colorPrimaryActive,
    cardGutter: token.marginXXS / 2
  };
};
// ============================== Export ==============================
exports.prepareComponentToken = prepareComponentToken;
var _default = exports.default = (0, _internal.genStyleHooks)('Tabs', token => {
  const tabsToken = (0, _internal.mergeToken)(token, {
    // `cardPadding` is empty by default, so we could calculate with dynamic `cardHeight`
    tabsCardPadding: token.cardPadding,
    dropdownEdgeChildVerticalPadding: token.paddingXXS,
    tabsActiveTextShadow: '0 0 0.25px currentcolor',
    tabsDropdownHeight: 200,
    tabsDropdownWidth: 120,
    tabsHorizontalItemMargin: `0 0 0 ${(0, _cssinjs.unit)(token.horizontalItemGutter)}`,
    tabsHorizontalItemMarginRTL: `0 0 0 ${(0, _cssinjs.unit)(token.horizontalItemGutter)}`
  });
  return [genSizeStyle(tabsToken), genRtlStyle(tabsToken), genPositionStyle(tabsToken), genDropdownStyle(tabsToken), genCardStyle(tabsToken), genTabsStyle(tabsToken), (0, _motion.default)(tabsToken)];
}, prepareComponentToken);