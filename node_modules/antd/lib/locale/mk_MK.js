"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _mk_MK = _interopRequireDefault(require("rc-pagination/lib/locale/mk_MK"));
var _mk_MK2 = _interopRequireDefault(require("../calendar/locale/mk_MK"));
var _mk_MK3 = _interopRequireDefault(require("../date-picker/locale/mk_MK"));
var _mk_MK4 = _interopRequireDefault(require("../time-picker/locale/mk_MK"));
const localeValues = {
  locale: 'mk',
  Pagination: _mk_MK.default,
  DatePicker: _mk_MK3.default,
  TimePicker: _mk_MK4.default,
  Calendar: _mk_MK2.default,
  global: {
    placeholder: 'Ве молиме означете'
  },
  Table: {
    filterTitle: 'Мени за филтрирање',
    filterConfirm: 'ОК',
    filterReset: 'Избриши',
    selectAll: 'Одбери страница',
    selectInvert: 'Инвертирај страница'
  },
  Modal: {
    okText: 'ОК',
    cancelText: 'Откажи',
    justOkText: 'ОК'
  },
  Popconfirm: {
    okText: 'ОК',
    cancelText: 'Откажи'
  },
  Transfer: {
    titles: ['', ''],
    searchPlaceholder: 'Пребарај тука',
    itemUnit: 'предмет',
    itemsUnit: 'предмети'
  },
  Upload: {
    uploading: 'Се прикачува...',
    removeFile: 'Избриши фајл',
    uploadError: 'Грешка при прикачување',
    previewFile: 'Прикажи фајл',
    downloadFile: 'Преземи фајл'
  },
  Empty: {
    description: 'Нема податоци'
  },
  Icon: {
    icon: 'Икона'
  },
  Text: {
    edit: 'Уреди',
    copy: 'Копирај',
    copied: 'Копирано',
    expand: 'Зголеми'
  }
};
var _default = exports.default = localeValues;