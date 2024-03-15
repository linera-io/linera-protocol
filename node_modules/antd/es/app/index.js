"use client";

import React, { useContext } from 'react';
import classNames from 'classnames';
import { ConfigContext } from '../config-provider';
import useMessage from '../message/useMessage';
import useModal from '../modal/useModal';
import useNotification from '../notification/useNotification';
import AppContext, { AppConfigContext } from './context';
import useStyle from './style';
const useApp = () => React.useContext(AppContext);
const App = props => {
  const {
    prefixCls: customizePrefixCls,
    children,
    className,
    rootClassName,
    message,
    notification,
    style,
    component = 'div'
  } = props;
  const {
    getPrefixCls
  } = useContext(ConfigContext);
  const prefixCls = getPrefixCls('app', customizePrefixCls);
  const [wrapCSSVar, hashId, cssVarCls] = useStyle(prefixCls);
  const customClassName = classNames(hashId, prefixCls, className, rootClassName, cssVarCls);
  const appConfig = useContext(AppConfigContext);
  const mergedAppConfig = React.useMemo(() => ({
    message: Object.assign(Object.assign({}, appConfig.message), message),
    notification: Object.assign(Object.assign({}, appConfig.notification), notification)
  }), [message, notification, appConfig.message, appConfig.notification]);
  const [messageApi, messageContextHolder] = useMessage(mergedAppConfig.message);
  const [notificationApi, notificationContextHolder] = useNotification(mergedAppConfig.notification);
  const [ModalApi, ModalContextHolder] = useModal();
  const memoizedContextValue = React.useMemo(() => ({
    message: messageApi,
    notification: notificationApi,
    modal: ModalApi
  }), [messageApi, notificationApi, ModalApi]);
  // ============================ Render ============================
  const Component = component === false ? React.Fragment : component;
  const rootProps = {
    className: customClassName,
    style
  };
  return wrapCSSVar( /*#__PURE__*/React.createElement(AppContext.Provider, {
    value: memoizedContextValue
  }, /*#__PURE__*/React.createElement(AppConfigContext.Provider, {
    value: mergedAppConfig
  }, /*#__PURE__*/React.createElement(Component, Object.assign({}, component === false ? undefined : rootProps), ModalContextHolder, messageContextHolder, notificationContextHolder, children))));
};
if (process.env.NODE_ENV !== 'production') {
  App.displayName = 'App';
}
App.useApp = useApp;
export default App;