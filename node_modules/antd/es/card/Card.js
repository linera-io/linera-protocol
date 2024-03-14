"use client";

var __rest = this && this.__rest || function (s, e) {
  var t = {};
  for (var p in s) if (Object.prototype.hasOwnProperty.call(s, p) && e.indexOf(p) < 0) t[p] = s[p];
  if (s != null && typeof Object.getOwnPropertySymbols === "function") for (var i = 0, p = Object.getOwnPropertySymbols(s); i < p.length; i++) {
    if (e.indexOf(p[i]) < 0 && Object.prototype.propertyIsEnumerable.call(s, p[i])) t[p[i]] = s[p[i]];
  }
  return t;
};
import * as React from 'react';
import classNames from 'classnames';
import omit from "rc-util/es/omit";
import { devUseWarning } from '../_util/warning';
import { ConfigContext } from '../config-provider';
import useSize from '../config-provider/hooks/useSize';
import Skeleton from '../skeleton';
import Tabs from '../tabs';
import Grid from './Grid';
import useStyle from './style';
const ActionNode = props => {
  const {
    actionClasses,
    actions = [],
    actionStyle
  } = props;
  return /*#__PURE__*/React.createElement("ul", {
    className: actionClasses,
    style: actionStyle
  }, actions.map((action, index) => {
    // Move this out since eslint not allow index key
    // And eslint-disable makes conflict with rollup
    // ref https://github.com/ant-design/ant-design/issues/46022
    const key = `action-${index}`;
    return /*#__PURE__*/React.createElement("li", {
      style: {
        width: `${100 / actions.length}%`
      },
      key: key
    }, /*#__PURE__*/React.createElement("span", null, action));
  }));
};
const Card = /*#__PURE__*/React.forwardRef((props, ref) => {
  const {
      prefixCls: customizePrefixCls,
      className,
      rootClassName,
      style,
      extra,
      headStyle = {},
      bodyStyle = {},
      title,
      loading,
      bordered = true,
      size: customizeSize,
      type,
      cover,
      actions,
      tabList,
      children,
      activeTabKey,
      defaultActiveTabKey,
      tabBarExtraContent,
      hoverable,
      tabProps = {},
      classNames: customClassNames,
      styles: customStyles
    } = props,
    others = __rest(props, ["prefixCls", "className", "rootClassName", "style", "extra", "headStyle", "bodyStyle", "title", "loading", "bordered", "size", "type", "cover", "actions", "tabList", "children", "activeTabKey", "defaultActiveTabKey", "tabBarExtraContent", "hoverable", "tabProps", "classNames", "styles"]);
  const {
    getPrefixCls,
    direction,
    card
  } = React.useContext(ConfigContext);
  // =================Warning===================
  if (process.env.NODE_ENV !== 'production') {
    const warning = devUseWarning('Card');
    [['headStyle', 'styles.header'], ['bodyStyle', 'styles.body']].forEach(_ref => {
      let [deprecatedName, newName] = _ref;
      warning.deprecated(!(deprecatedName in props), deprecatedName, newName);
    });
  }
  const onTabChange = key => {
    var _a;
    (_a = props.onTabChange) === null || _a === void 0 ? void 0 : _a.call(props, key);
  };
  const moduleClass = moduleName => {
    var _a;
    return classNames((_a = card === null || card === void 0 ? void 0 : card.classNames) === null || _a === void 0 ? void 0 : _a[moduleName], customClassNames === null || customClassNames === void 0 ? void 0 : customClassNames[moduleName]);
  };
  const moduleStyle = moduleName => {
    var _a;
    return Object.assign(Object.assign({}, (_a = card === null || card === void 0 ? void 0 : card.styles) === null || _a === void 0 ? void 0 : _a[moduleName]), customStyles === null || customStyles === void 0 ? void 0 : customStyles[moduleName]);
  };
  const isContainGrid = React.useMemo(() => {
    let containGrid = false;
    React.Children.forEach(children, element => {
      if (element && element.type && element.type === Grid) {
        containGrid = true;
      }
    });
    return containGrid;
  }, [children]);
  const prefixCls = getPrefixCls('card', customizePrefixCls);
  const [wrapCSSVar, hashId, cssVarCls] = useStyle(prefixCls);
  const loadingBlock = /*#__PURE__*/React.createElement(Skeleton, {
    loading: true,
    active: true,
    paragraph: {
      rows: 4
    },
    title: false
  }, children);
  const hasActiveTabKey = activeTabKey !== undefined;
  const extraProps = Object.assign(Object.assign({}, tabProps), {
    [hasActiveTabKey ? 'activeKey' : 'defaultActiveKey']: hasActiveTabKey ? activeTabKey : defaultActiveTabKey,
    tabBarExtraContent
  });
  let head;
  const mergedSize = useSize(customizeSize);
  const tabSize = !mergedSize || mergedSize === 'default' ? 'large' : mergedSize;
  const tabs = tabList ? ( /*#__PURE__*/React.createElement(Tabs, Object.assign({
    size: tabSize
  }, extraProps, {
    className: `${prefixCls}-head-tabs`,
    onChange: onTabChange,
    items: tabList.map(_a => {
      var {
          tab
        } = _a,
        item = __rest(_a, ["tab"]);
      return Object.assign({
        label: tab
      }, item);
    })
  }))) : null;
  if (title || extra || tabs) {
    const headClasses = classNames(`${prefixCls}-head`, moduleClass('header'));
    const titleClasses = classNames(`${prefixCls}-head-title`, moduleClass('title'));
    const extraClasses = classNames(`${prefixCls}-extra`, moduleClass('extra'));
    const mergedHeadStyle = Object.assign(Object.assign({}, headStyle), moduleStyle('header'));
    head = /*#__PURE__*/React.createElement("div", {
      className: headClasses,
      style: mergedHeadStyle
    }, /*#__PURE__*/React.createElement("div", {
      className: `${prefixCls}-head-wrapper`
    }, title && ( /*#__PURE__*/React.createElement("div", {
      className: titleClasses,
      style: moduleStyle('title')
    }, title)), extra && ( /*#__PURE__*/React.createElement("div", {
      className: extraClasses,
      style: moduleStyle('extra')
    }, extra))), tabs);
  }
  const coverClasses = classNames(`${prefixCls}-cover`, moduleClass('cover'));
  const coverDom = cover ? ( /*#__PURE__*/React.createElement("div", {
    className: coverClasses,
    style: moduleStyle('cover')
  }, cover)) : null;
  const bodyClasses = classNames(`${prefixCls}-body`, moduleClass('body'));
  const mergedBodyStyle = Object.assign(Object.assign({}, bodyStyle), moduleStyle('body'));
  const body = /*#__PURE__*/React.createElement("div", {
    className: bodyClasses,
    style: mergedBodyStyle
  }, loading ? loadingBlock : children);
  const actionClasses = classNames(`${prefixCls}-actions`, moduleClass('actions'));
  const actionDom = actions && actions.length ? ( /*#__PURE__*/React.createElement(ActionNode, {
    actionClasses: actionClasses,
    actionStyle: moduleStyle('actions'),
    actions: actions
  })) : null;
  const divProps = omit(others, ['onTabChange']);
  const classString = classNames(prefixCls, card === null || card === void 0 ? void 0 : card.className, {
    [`${prefixCls}-loading`]: loading,
    [`${prefixCls}-bordered`]: bordered,
    [`${prefixCls}-hoverable`]: hoverable,
    [`${prefixCls}-contain-grid`]: isContainGrid,
    [`${prefixCls}-contain-tabs`]: tabList && tabList.length,
    [`${prefixCls}-${mergedSize}`]: mergedSize,
    [`${prefixCls}-type-${type}`]: !!type,
    [`${prefixCls}-rtl`]: direction === 'rtl'
  }, className, rootClassName, hashId, cssVarCls);
  const mergedStyle = Object.assign(Object.assign({}, card === null || card === void 0 ? void 0 : card.style), style);
  return wrapCSSVar( /*#__PURE__*/React.createElement("div", Object.assign({
    ref: ref
  }, divProps, {
    className: classString,
    style: mergedStyle
  }), head, coverDom, body, actionDom));
});
export default Card;