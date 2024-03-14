"use strict";
"use client";

var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard").default;
var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _classnames = _interopRequireDefault(require("classnames"));
var React = _interopRequireWildcard(require("react"));
var _configProvider = require("../config-provider");
var _Avatar = _interopRequireDefault(require("./Avatar"));
var _Button = _interopRequireDefault(require("./Button"));
var _Element = _interopRequireDefault(require("./Element"));
var _Image = _interopRequireDefault(require("./Image"));
var _Input = _interopRequireDefault(require("./Input"));
var _Node = _interopRequireDefault(require("./Node"));
var _Paragraph = _interopRequireDefault(require("./Paragraph"));
var _Title = _interopRequireDefault(require("./Title"));
var _style = _interopRequireDefault(require("./style"));
function getComponentProps(prop) {
  if (prop && typeof prop === 'object') {
    return prop;
  }
  return {};
}
function getAvatarBasicProps(hasTitle, hasParagraph) {
  if (hasTitle && !hasParagraph) {
    // Square avatar
    return {
      size: 'large',
      shape: 'square'
    };
  }
  return {
    size: 'large',
    shape: 'circle'
  };
}
function getTitleBasicProps(hasAvatar, hasParagraph) {
  if (!hasAvatar && hasParagraph) {
    return {
      width: '38%'
    };
  }
  if (hasAvatar && hasParagraph) {
    return {
      width: '50%'
    };
  }
  return {};
}
function getParagraphBasicProps(hasAvatar, hasTitle) {
  const basicProps = {};
  // Width
  if (!hasAvatar || !hasTitle) {
    basicProps.width = '61%';
  }
  // Rows
  if (!hasAvatar && hasTitle) {
    basicProps.rows = 3;
  } else {
    basicProps.rows = 2;
  }
  return basicProps;
}
const Skeleton = props => {
  const {
    prefixCls: customizePrefixCls,
    loading,
    className,
    rootClassName,
    style,
    children,
    avatar = false,
    title = true,
    paragraph = true,
    active,
    round
  } = props;
  const {
    getPrefixCls,
    direction,
    skeleton
  } = React.useContext(_configProvider.ConfigContext);
  const prefixCls = getPrefixCls('skeleton', customizePrefixCls);
  const [wrapCSSVar, hashId, cssVarCls] = (0, _style.default)(prefixCls);
  if (loading || !('loading' in props)) {
    const hasAvatar = !!avatar;
    const hasTitle = !!title;
    const hasParagraph = !!paragraph;
    // Avatar
    let avatarNode;
    if (hasAvatar) {
      const avatarProps = Object.assign(Object.assign({
        prefixCls: `${prefixCls}-avatar`
      }, getAvatarBasicProps(hasTitle, hasParagraph)), getComponentProps(avatar));
      // We direct use SkeletonElement as avatar in skeleton internal.
      avatarNode = /*#__PURE__*/React.createElement("div", {
        className: `${prefixCls}-header`
      }, /*#__PURE__*/React.createElement(_Element.default, Object.assign({}, avatarProps)));
    }
    let contentNode;
    if (hasTitle || hasParagraph) {
      // Title
      let $title;
      if (hasTitle) {
        const titleProps = Object.assign(Object.assign({
          prefixCls: `${prefixCls}-title`
        }, getTitleBasicProps(hasAvatar, hasParagraph)), getComponentProps(title));
        $title = /*#__PURE__*/React.createElement(_Title.default, Object.assign({}, titleProps));
      }
      // Paragraph
      let paragraphNode;
      if (hasParagraph) {
        const paragraphProps = Object.assign(Object.assign({
          prefixCls: `${prefixCls}-paragraph`
        }, getParagraphBasicProps(hasAvatar, hasTitle)), getComponentProps(paragraph));
        paragraphNode = /*#__PURE__*/React.createElement(_Paragraph.default, Object.assign({}, paragraphProps));
      }
      contentNode = /*#__PURE__*/React.createElement("div", {
        className: `${prefixCls}-content`
      }, $title, paragraphNode);
    }
    const cls = (0, _classnames.default)(prefixCls, {
      [`${prefixCls}-with-avatar`]: hasAvatar,
      [`${prefixCls}-active`]: active,
      [`${prefixCls}-rtl`]: direction === 'rtl',
      [`${prefixCls}-round`]: round
    }, skeleton === null || skeleton === void 0 ? void 0 : skeleton.className, className, rootClassName, hashId, cssVarCls);
    return wrapCSSVar( /*#__PURE__*/React.createElement("div", {
      className: cls,
      style: Object.assign(Object.assign({}, skeleton === null || skeleton === void 0 ? void 0 : skeleton.style), style)
    }, avatarNode, contentNode));
  }
  return typeof children !== 'undefined' ? children : null;
};
Skeleton.Button = _Button.default;
Skeleton.Avatar = _Avatar.default;
Skeleton.Input = _Input.default;
Skeleton.Image = _Image.default;
Skeleton.Node = _Node.default;
if (process.env.NODE_ENV !== 'production') {
  Skeleton.displayName = 'Skeleton';
}
var _default = exports.default = Skeleton;