"use strict";
"use client";

var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard").default;
var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _DeleteOutlined = _interopRequireDefault(require("@ant-design/icons/DeleteOutlined"));
var _DownloadOutlined = _interopRequireDefault(require("@ant-design/icons/DownloadOutlined"));
var _EyeOutlined = _interopRequireDefault(require("@ant-design/icons/EyeOutlined"));
var _classnames = _interopRequireDefault(require("classnames"));
var _rcMotion = _interopRequireDefault(require("rc-motion"));
var React = _interopRequireWildcard(require("react"));
var _configProvider = require("../../config-provider");
var _progress = _interopRequireDefault(require("../../progress"));
var _tooltip = _interopRequireDefault(require("../../tooltip"));
const ListItem = /*#__PURE__*/React.forwardRef((_ref, ref) => {
  let {
    prefixCls,
    className,
    style,
    locale,
    listType,
    file,
    items,
    progress: progressProps,
    iconRender,
    actionIconRender,
    itemRender,
    isImgUrl,
    showPreviewIcon,
    showRemoveIcon,
    showDownloadIcon,
    previewIcon: customPreviewIcon,
    removeIcon: customRemoveIcon,
    downloadIcon: customDownloadIcon,
    onPreview,
    onDownload,
    onClose
  } = _ref;
  var _a, _b;
  // Status: which will ignore `removed` status
  const {
    status
  } = file;
  const [mergedStatus, setMergedStatus] = React.useState(status);
  React.useEffect(() => {
    if (status !== 'removed') {
      setMergedStatus(status);
    }
  }, [status]);
  // Delay to show the progress bar
  const [showProgress, setShowProgress] = React.useState(false);
  React.useEffect(() => {
    const timer = setTimeout(() => {
      setShowProgress(true);
    }, 300);
    return () => {
      clearTimeout(timer);
    };
  }, []);
  const iconNode = iconRender(file);
  let icon = /*#__PURE__*/React.createElement("div", {
    className: `${prefixCls}-icon`
  }, iconNode);
  if (listType === 'picture' || listType === 'picture-card' || listType === 'picture-circle') {
    if (mergedStatus === 'uploading' || !file.thumbUrl && !file.url) {
      const uploadingClassName = (0, _classnames.default)(`${prefixCls}-list-item-thumbnail`, {
        [`${prefixCls}-list-item-file`]: mergedStatus !== 'uploading'
      });
      icon = /*#__PURE__*/React.createElement("div", {
        className: uploadingClassName
      }, iconNode);
    } else {
      const thumbnail = (isImgUrl === null || isImgUrl === void 0 ? void 0 : isImgUrl(file)) ? ( /*#__PURE__*/React.createElement("img", {
        src: file.thumbUrl || file.url,
        alt: file.name,
        className: `${prefixCls}-list-item-image`,
        crossOrigin: file.crossOrigin
      })) : iconNode;
      const aClassName = (0, _classnames.default)(`${prefixCls}-list-item-thumbnail`, {
        [`${prefixCls}-list-item-file`]: isImgUrl && !isImgUrl(file)
      });
      icon = /*#__PURE__*/React.createElement("a", {
        className: aClassName,
        onClick: e => onPreview(file, e),
        href: file.url || file.thumbUrl,
        target: "_blank",
        rel: "noopener noreferrer"
      }, thumbnail);
    }
  }
  const listItemClassName = (0, _classnames.default)(`${prefixCls}-list-item`, `${prefixCls}-list-item-${mergedStatus}`);
  const linkProps = typeof file.linkProps === 'string' ? JSON.parse(file.linkProps) : file.linkProps;
  const removeIcon = showRemoveIcon ? actionIconRender((typeof customRemoveIcon === 'function' ? customRemoveIcon(file) : customRemoveIcon) || ( /*#__PURE__*/React.createElement(_DeleteOutlined.default, null)), () => onClose(file), prefixCls, locale.removeFile,
  // acceptUploadDisabled is true, only remove icon will follow Upload disabled prop
  // https://github.com/ant-design/ant-design/issues/46171
  true) : null;
  const downloadIcon = showDownloadIcon && mergedStatus === 'done' ? actionIconRender((typeof customDownloadIcon === 'function' ? customDownloadIcon(file) : customDownloadIcon) || /*#__PURE__*/React.createElement(_DownloadOutlined.default, null), () => onDownload(file), prefixCls, locale.downloadFile) : null;
  const downloadOrDelete = listType !== 'picture-card' && listType !== 'picture-circle' && ( /*#__PURE__*/React.createElement("span", {
    key: "download-delete",
    className: (0, _classnames.default)(`${prefixCls}-list-item-actions`, {
      picture: listType === 'picture'
    })
  }, downloadIcon, removeIcon));
  const listItemNameClass = (0, _classnames.default)(`${prefixCls}-list-item-name`);
  const fileName = file.url ? [/*#__PURE__*/React.createElement("a", Object.assign({
    key: "view",
    target: "_blank",
    rel: "noopener noreferrer",
    className: listItemNameClass,
    title: file.name
  }, linkProps, {
    href: file.url,
    onClick: e => onPreview(file, e)
  }), file.name), downloadOrDelete] : [/*#__PURE__*/React.createElement("span", {
    key: "view",
    className: listItemNameClass,
    onClick: e => onPreview(file, e),
    title: file.name
  }, file.name), downloadOrDelete];
  const previewIcon = showPreviewIcon && (file.url || file.thumbUrl) ? ( /*#__PURE__*/React.createElement("a", {
    href: file.url || file.thumbUrl,
    target: "_blank",
    rel: "noopener noreferrer",
    onClick: e => onPreview(file, e),
    title: locale.previewFile
  }, typeof customPreviewIcon === 'function' ? customPreviewIcon(file) : customPreviewIcon || /*#__PURE__*/React.createElement(_EyeOutlined.default, null))) : null;
  const pictureCardActions = (listType === 'picture-card' || listType === 'picture-circle') && mergedStatus !== 'uploading' && ( /*#__PURE__*/React.createElement("span", {
    className: `${prefixCls}-list-item-actions`
  }, previewIcon, mergedStatus === 'done' && downloadIcon, removeIcon));
  const {
    getPrefixCls
  } = React.useContext(_configProvider.ConfigContext);
  const rootPrefixCls = getPrefixCls();
  const dom = /*#__PURE__*/React.createElement("div", {
    className: listItemClassName
  }, icon, fileName, pictureCardActions, showProgress && ( /*#__PURE__*/React.createElement(_rcMotion.default, {
    motionName: `${rootPrefixCls}-fade`,
    visible: mergedStatus === 'uploading',
    motionDeadline: 2000
  }, _ref2 => {
    let {
      className: motionClassName
    } = _ref2;
    // show loading icon if upload progress listener is disabled
    const loadingProgress = 'percent' in file ? ( /*#__PURE__*/React.createElement(_progress.default, Object.assign({}, progressProps, {
      type: "line",
      percent: file.percent,
      "aria-label": file['aria-label'],
      "aria-labelledby": file['aria-labelledby']
    }))) : null;
    return /*#__PURE__*/React.createElement("div", {
      className: (0, _classnames.default)(`${prefixCls}-list-item-progress`, motionClassName)
    }, loadingProgress);
  })));
  const message = file.response && typeof file.response === 'string' ? file.response : ((_a = file.error) === null || _a === void 0 ? void 0 : _a.statusText) || ((_b = file.error) === null || _b === void 0 ? void 0 : _b.message) || locale.uploadError;
  const item = mergedStatus === 'error' ? ( /*#__PURE__*/React.createElement(_tooltip.default, {
    title: message,
    getPopupContainer: node => node.parentNode
  }, dom)) : dom;
  return /*#__PURE__*/React.createElement("div", {
    className: (0, _classnames.default)(`${prefixCls}-list-item-container`, className),
    style: style,
    ref: ref
  }, itemRender ? itemRender(item, file, items, {
    download: onDownload.bind(null, file),
    preview: onPreview.bind(null, file),
    remove: onClose.bind(null, file)
  }) : item);
});
var _default = exports.default = ListItem;