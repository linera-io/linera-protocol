"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _internal = require("../../theme/internal");
var _ = require(".");
var _cssinjs = require("@ant-design/cssinjs");
var _default = exports.default = (0, _internal.genSubStyleComponent)(['Notification', 'PurePanel'], token => {
  const noticeCls = `${token.componentCls}-notice`;
  const notificationToken = (0, _.prepareNotificationToken)(token);
  return {
    [`${noticeCls}-pure-panel`]: Object.assign(Object.assign({}, (0, _.genNoticeStyle)(notificationToken)), {
      width: notificationToken.width,
      maxWidth: `calc(100vw - ${(0, _cssinjs.unit)(token.calc(notificationToken.notificationMarginEdge).mul(2).equal())})`,
      margin: 0
    })
  };
}, _.prepareComponentToken);