"use strict";
"use client";

var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard").default;
var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _toConsumableArray2 = _interopRequireDefault(require("@babel/runtime/helpers/toConsumableArray"));
var React = _interopRequireWildcard(require("react"));
var _classnames = _interopRequireDefault(require("classnames"));
var _rcUtil = require("rc-util");
var _scrollIntoViewIfNeeded = _interopRequireDefault(require("scroll-into-view-if-needed"));
var _getScroll = _interopRequireDefault(require("../_util/getScroll"));
var _scrollTo = _interopRequireDefault(require("../_util/scrollTo"));
var _warning = require("../_util/warning");
var _affix = _interopRequireDefault(require("../affix"));
var _configProvider = require("../config-provider");
var _useCSSVarCls = _interopRequireDefault(require("../config-provider/hooks/useCSSVarCls"));
var _AnchorLink = _interopRequireDefault(require("./AnchorLink"));
var _context = _interopRequireDefault(require("./context"));
var _style = _interopRequireDefault(require("./style"));
function getDefaultContainer() {
  return window;
}
function getOffsetTop(element, container) {
  if (!element.getClientRects().length) {
    return 0;
  }
  const rect = element.getBoundingClientRect();
  if (rect.width || rect.height) {
    if (container === window) {
      container = element.ownerDocument.documentElement;
      return rect.top - container.clientTop;
    }
    return rect.top - container.getBoundingClientRect().top;
  }
  return rect.top;
}
const sharpMatcherRegex = /#([\S ]+)$/;
const Anchor = props => {
  var _a;
  const {
    rootClassName,
    prefixCls: customPrefixCls,
    className,
    style,
    offsetTop,
    affix = true,
    showInkInFixed = false,
    children,
    items,
    direction: anchorDirection = 'vertical',
    bounds,
    targetOffset,
    onClick,
    onChange,
    getContainer,
    getCurrentAnchor,
    replace
  } = props;
  // =================== Warning =====================
  if (process.env.NODE_ENV !== 'production') {
    const warning = (0, _warning.devUseWarning)('Anchor');
    warning.deprecated(!children, 'Anchor children', 'items');
    process.env.NODE_ENV !== "production" ? warning(!(anchorDirection === 'horizontal' && (items === null || items === void 0 ? void 0 : items.some(n => 'children' in n))), 'usage', '`Anchor items#children` is not supported when `Anchor` direction is horizontal.') : void 0;
  }
  const [links, setLinks] = React.useState([]);
  const [activeLink, setActiveLink] = React.useState(null);
  const activeLinkRef = React.useRef(activeLink);
  const wrapperRef = React.useRef(null);
  const spanLinkNode = React.useRef(null);
  const animating = React.useRef(false);
  const {
    direction,
    anchor,
    getTargetContainer,
    getPrefixCls
  } = React.useContext(_configProvider.ConfigContext);
  const prefixCls = getPrefixCls('anchor', customPrefixCls);
  const rootCls = (0, _useCSSVarCls.default)(prefixCls);
  const [wrapCSSVar, hashId, cssVarCls] = (0, _style.default)(prefixCls, rootCls);
  const getCurrentContainer = (_a = getContainer !== null && getContainer !== void 0 ? getContainer : getTargetContainer) !== null && _a !== void 0 ? _a : getDefaultContainer;
  const dependencyListItem = JSON.stringify(links);
  const registerLink = (0, _rcUtil.useEvent)(link => {
    if (!links.includes(link)) {
      setLinks(prev => [].concat((0, _toConsumableArray2.default)(prev), [link]));
    }
  });
  const unregisterLink = (0, _rcUtil.useEvent)(link => {
    if (links.includes(link)) {
      setLinks(prev => prev.filter(i => i !== link));
    }
  });
  const updateInk = () => {
    var _a;
    const linkNode = (_a = wrapperRef.current) === null || _a === void 0 ? void 0 : _a.querySelector(`.${prefixCls}-link-title-active`);
    if (linkNode && spanLinkNode.current) {
      const {
        style: inkStyle
      } = spanLinkNode.current;
      const horizontalAnchor = anchorDirection === 'horizontal';
      inkStyle.top = horizontalAnchor ? '' : `${linkNode.offsetTop + linkNode.clientHeight / 2}px`;
      inkStyle.height = horizontalAnchor ? '' : `${linkNode.clientHeight}px`;
      inkStyle.left = horizontalAnchor ? `${linkNode.offsetLeft}px` : '';
      inkStyle.width = horizontalAnchor ? `${linkNode.clientWidth}px` : '';
      if (horizontalAnchor) {
        (0, _scrollIntoViewIfNeeded.default)(linkNode, {
          scrollMode: 'if-needed',
          block: 'nearest'
        });
      }
    }
  };
  const getInternalCurrentAnchor = function (_links) {
    let _offsetTop = arguments.length > 1 && arguments[1] !== undefined ? arguments[1] : 0;
    let _bounds = arguments.length > 2 && arguments[2] !== undefined ? arguments[2] : 5;
    const linkSections = [];
    const container = getCurrentContainer();
    _links.forEach(link => {
      const sharpLinkMatch = sharpMatcherRegex.exec(link === null || link === void 0 ? void 0 : link.toString());
      if (!sharpLinkMatch) {
        return;
      }
      const target = document.getElementById(sharpLinkMatch[1]);
      if (target) {
        const top = getOffsetTop(target, container);
        if (top <= _offsetTop + _bounds) {
          linkSections.push({
            link,
            top
          });
        }
      }
    });
    if (linkSections.length) {
      const maxSection = linkSections.reduce((prev, curr) => curr.top > prev.top ? curr : prev);
      return maxSection.link;
    }
    return '';
  };
  const setCurrentActiveLink = (0, _rcUtil.useEvent)(link => {
    // FIXME: Seems a bug since this compare is not equals
    // `activeLinkRef` is parsed value which will always trigger `onChange` event.
    if (activeLinkRef.current === link) {
      return;
    }
    // https://github.com/ant-design/ant-design/issues/30584
    const newLink = typeof getCurrentAnchor === 'function' ? getCurrentAnchor(link) : link;
    setActiveLink(newLink);
    activeLinkRef.current = newLink;
    // onChange should respect the original link (which may caused by
    // window scroll or user click), not the new link
    onChange === null || onChange === void 0 ? void 0 : onChange(link);
  });
  const handleScroll = React.useCallback(() => {
    if (animating.current) {
      return;
    }
    const currentActiveLink = getInternalCurrentAnchor(links, targetOffset !== undefined ? targetOffset : offsetTop || 0, bounds);
    setCurrentActiveLink(currentActiveLink);
  }, [dependencyListItem, targetOffset, offsetTop]);
  const handleScrollTo = React.useCallback(link => {
    setCurrentActiveLink(link);
    const sharpLinkMatch = sharpMatcherRegex.exec(link);
    if (!sharpLinkMatch) {
      return;
    }
    const targetElement = document.getElementById(sharpLinkMatch[1]);
    if (!targetElement) {
      return;
    }
    const container = getCurrentContainer();
    const scrollTop = (0, _getScroll.default)(container, true);
    const eleOffsetTop = getOffsetTop(targetElement, container);
    let y = scrollTop + eleOffsetTop;
    y -= targetOffset !== undefined ? targetOffset : offsetTop || 0;
    animating.current = true;
    (0, _scrollTo.default)(y, {
      getContainer: getCurrentContainer,
      callback() {
        animating.current = false;
      }
    });
  }, [targetOffset, offsetTop]);
  const wrapperClass = (0, _classnames.default)(hashId, cssVarCls, rootCls, rootClassName, `${prefixCls}-wrapper`, {
    [`${prefixCls}-wrapper-horizontal`]: anchorDirection === 'horizontal',
    [`${prefixCls}-rtl`]: direction === 'rtl'
  }, className, anchor === null || anchor === void 0 ? void 0 : anchor.className);
  const anchorClass = (0, _classnames.default)(prefixCls, {
    [`${prefixCls}-fixed`]: !affix && !showInkInFixed
  });
  const inkClass = (0, _classnames.default)(`${prefixCls}-ink`, {
    [`${prefixCls}-ink-visible`]: activeLink
  });
  const wrapperStyle = Object.assign(Object.assign({
    maxHeight: offsetTop ? `calc(100vh - ${offsetTop}px)` : '100vh'
  }, anchor === null || anchor === void 0 ? void 0 : anchor.style), style);
  const createNestedLink = options => Array.isArray(options) ? options.map(item => ( /*#__PURE__*/React.createElement(_AnchorLink.default, Object.assign({
    replace: replace
  }, item, {
    key: item.key
  }), anchorDirection === 'vertical' && createNestedLink(item.children)))) : null;
  const anchorContent = /*#__PURE__*/React.createElement("div", {
    ref: wrapperRef,
    className: wrapperClass,
    style: wrapperStyle
  }, /*#__PURE__*/React.createElement("div", {
    className: anchorClass
  }, /*#__PURE__*/React.createElement("span", {
    className: inkClass,
    ref: spanLinkNode
  }), 'items' in props ? createNestedLink(items) : children));
  React.useEffect(() => {
    const scrollContainer = getCurrentContainer();
    handleScroll();
    scrollContainer === null || scrollContainer === void 0 ? void 0 : scrollContainer.addEventListener('scroll', handleScroll);
    return () => {
      scrollContainer === null || scrollContainer === void 0 ? void 0 : scrollContainer.removeEventListener('scroll', handleScroll);
    };
  }, [dependencyListItem]);
  React.useEffect(() => {
    if (typeof getCurrentAnchor === 'function') {
      setCurrentActiveLink(getCurrentAnchor(activeLinkRef.current || ''));
    }
  }, [getCurrentAnchor]);
  React.useEffect(() => {
    updateInk();
  }, [anchorDirection, getCurrentAnchor, dependencyListItem, activeLink]);
  const memoizedContextValue = React.useMemo(() => ({
    registerLink,
    unregisterLink,
    scrollTo: handleScrollTo,
    activeLink,
    onClick,
    direction: anchorDirection
  }), [activeLink, onClick, handleScrollTo, anchorDirection]);
  return wrapCSSVar( /*#__PURE__*/React.createElement(_context.default.Provider, {
    value: memoizedContextValue
  }, affix ? ( /*#__PURE__*/React.createElement(_affix.default, {
    offsetTop: offsetTop,
    target: getCurrentContainer
  }, anchorContent)) : anchorContent));
};
if (process.env.NODE_ENV !== 'production') {
  Anchor.displayName = 'Anchor';
}
var _default = exports.default = Anchor;