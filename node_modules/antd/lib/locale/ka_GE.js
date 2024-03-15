"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _ka_GE = _interopRequireDefault(require("rc-pagination/lib/locale/ka_GE"));
var _ka_GE2 = _interopRequireDefault(require("../calendar/locale/ka_GE"));
var _ka_GE3 = _interopRequireDefault(require("../date-picker/locale/ka_GE"));
var _ka_GE4 = _interopRequireDefault(require("../time-picker/locale/ka_GE"));
/* eslint-disable no-template-curly-in-string */

const typeTemplate = '${label} არ არის სწორი ${type}';
const localeValues = {
  locale: 'ka',
  Pagination: _ka_GE.default,
  DatePicker: _ka_GE3.default,
  TimePicker: _ka_GE4.default,
  Calendar: _ka_GE2.default,
  global: {
    placeholder: 'გთხოვთ აირჩიოთ'
  },
  Table: {
    filterTitle: 'ფილტრის მენიუ',
    filterConfirm: 'კარგი',
    filterReset: 'გასუფთავება',
    filterEmptyText: 'ფილტრები არაა',
    emptyText: 'ინფორმაცია არაა',
    selectAll: 'აირჩიეთ მიმდინარე გვერდი',
    selectInvert: 'შეაბრუნეთ მიმდინარე გვერდი',
    selectNone: 'მონაცემების გასუფთავება',
    selectionAll: 'ყველას მონიშვნა',
    sortTitle: 'დალაგება',
    expand: 'სტრიქონის გაშლა',
    collapse: 'სტრიქონის შეკუმშვა',
    triggerDesc: 'დაღმავალი დალაგება',
    triggerAsc: 'აღმავალი დალაგება',
    cancelSort: 'დალაგების გაუქმება'
  },
  Modal: {
    okText: 'კარგი',
    cancelText: 'გაუქმება',
    justOkText: 'ოკ'
  },
  Popconfirm: {
    okText: 'კარგი',
    cancelText: 'გაუქმება'
  },
  Transfer: {
    titles: ['', ''],
    searchPlaceholder: 'მოძებნე აქ',
    itemUnit: 'ერთეული',
    itemsUnit: 'ერთეულები',
    remove: 'ამოშლა',
    selectCurrent: 'მიმდინარე გვერდის არჩევა',
    removeCurrent: 'მიმდინარე გვერდის ამოშლა',
    selectAll: 'ყველას მონიშვნა',
    removeAll: 'ყველას წაშლა',
    selectInvert: 'მიმდინარე გვერდის შებრუნება'
  },
  Upload: {
    uploading: 'იტვირთება...',
    removeFile: 'ფაილის ამოშლა',
    uploadError: 'ატვირთვის შეცდომა',
    previewFile: 'ფაილის გადახედვა',
    downloadFile: 'ფაილის ჩამოტვირთვა'
  },
  Empty: {
    description: 'ინფორმაცია არაა'
  },
  Icon: {
    icon: 'ხატულა'
  },
  Text: {
    edit: 'რედაქტირება',
    copy: 'ასლი',
    copied: 'ასლი აღებულია',
    expand: 'გაშლა'
  },
  Form: {
    optional: '(არასავალდებულო)',
    defaultValidateMessages: {
      default: 'ველის შემოწმების შეცდომა ${label}-ისთვის',
      required: 'გთხოვთ შეიყვანეთ ${label}',
      enum: '${label} უნდა იყოს ერთ-ერთი [${enum}]-დან',
      whitespace: '${label} არ შეიძლება იყოს ცარიელი სიმბოლო',
      date: {
        format: '${label} თარიღის ფორმატი არასწორია',
        parse: '${label} თარიღში კონვერტირება არ არის შესაძლებელი',
        invalid: '${label} არასწორი თარიღია'
      },
      types: {
        string: typeTemplate,
        method: typeTemplate,
        array: typeTemplate,
        object: typeTemplate,
        number: typeTemplate,
        date: typeTemplate,
        boolean: typeTemplate,
        integer: typeTemplate,
        float: typeTemplate,
        regexp: typeTemplate,
        email: typeTemplate,
        url: typeTemplate,
        hex: typeTemplate
      },
      string: {
        len: '${label} უნდა იყოს ${len} სიმბოლო',
        min: '${label} უნდა იყოს სულ მცირე ${min} სიმბოლო',
        max: '${label} უნდა იყოს მაქსიმუმ ${max} სიმბოლო',
        range: '${label} უნდა იყოს ${min}-${max} სიმბოლოს შორის'
      },
      number: {
        len: '${label} უნდა იყოს ${len} ტოლი',
        min: '${label} უნდა იყოს მინუმიმ ${min}',
        max: '${label} უნდა იყოს მაქსიმუმ ${max}',
        range: '${label} უნდა იყოს ${min}-${max} შორის'
      },
      array: {
        len: 'უნდა იყოს ${len} ${label}',
        min: 'სულ მცირე ${min} ${label}',
        max: 'არაუმეტეს ${max} ${label}',
        range: '${label}-ის რაოდენობა უნდა იყოს ${min}-${max} შორის'
      },
      pattern: {
        mismatch: '${label} არ ერგება შაბლონს ${pattern}'
      }
    }
  },
  Image: {
    preview: 'გადახედვა'
  }
};
var _default = exports.default = localeValues;