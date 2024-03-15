"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _ms_MY = _interopRequireDefault(require("rc-pagination/lib/locale/ms_MY"));
var _ms_MY2 = _interopRequireDefault(require("../calendar/locale/ms_MY"));
var _ms_MY3 = _interopRequireDefault(require("../date-picker/locale/ms_MY"));
var _ms_MY4 = _interopRequireDefault(require("../time-picker/locale/ms_MY"));
const localeValues = {
  locale: 'ms-my',
  Pagination: _ms_MY.default,
  DatePicker: _ms_MY3.default,
  TimePicker: _ms_MY4.default,
  Calendar: _ms_MY2.default,
  global: {
    placeholder: 'Sila pilih'
  },
  Text: {
    edit: 'Sunting',
    copy: 'Salin',
    copied: 'Berjaya menyalin',
    expand: 'Kembang'
  },
  Empty: {
    description: 'Tiada data'
  },
  Table: {
    filterTitle: 'Cari dengan tajuk',
    filterConfirm: 'OK',
    filterReset: 'Menetapkan semula',
    emptyText: 'Tiada data',
    selectAll: 'Pilih semua',
    selectInvert: 'Terbalikkan'
  },
  Modal: {
    okText: 'OK',
    cancelText: 'Batal',
    justOkText: 'OK'
  },
  Popconfirm: {
    okText: 'OK',
    cancelText: 'Batal'
  },
  Transfer: {
    titles: ['', ''],
    notFoundContent: 'Tidak dijumpai',
    searchPlaceholder: 'Carian di sini',
    itemUnit: 'item',
    itemsUnit: 'item'
  },
  Icon: {
    icon: 'ikon'
  },
  Select: {
    notFoundContent: 'Tidak Dijumpai'
  },
  Upload: {
    uploading: 'Sedang memuat naik...',
    removeFile: 'Buang fail',
    uploadError: 'Masalah muat naik',
    previewFile: 'Tengok fail',
    downloadFile: 'Muat turun fail'
  }
};
var _default = exports.default = localeValues;