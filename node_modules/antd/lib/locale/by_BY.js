"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _by_BY = _interopRequireDefault(require("rc-pagination/lib/locale/by_BY"));
var _by_BY2 = _interopRequireDefault(require("../calendar/locale/by_BY"));
var _by_BY3 = _interopRequireDefault(require("../date-picker/locale/by_BY"));
var _by_BY4 = _interopRequireDefault(require("../time-picker/locale/by_BY"));
/* eslint-disable no-template-curly-in-string */

const typeTemplate = "${label} не з'яўляецца тыпам ${type}";
const localeValues = {
  locale: 'by',
  Pagination: _by_BY.default,
  DatePicker: _by_BY3.default,
  TimePicker: _by_BY4.default,
  Calendar: _by_BY2.default,
  global: {
    placeholder: 'Калі ласка, выберыце'
  },
  Table: {
    filterTitle: 'Фільтр',
    filterConfirm: 'OK',
    filterReset: 'Скінуць',
    filterEmptyText: 'Без фільтраў',
    filterCheckall: 'Выбраць усё',
    filterSearchPlaceholder: 'Пошук фільтраў',
    emptyText: 'Няма даных',
    selectAll: 'Выбраць усё',
    selectInvert: 'Інвертаваць выбар',
    selectNone: 'Ачысціць усе даныя',
    selectionAll: 'Выбраць усе даныя',
    sortTitle: 'Сартаванне',
    expand: 'Разгарнуць радок',
    collapse: 'Згарнуць радок',
    triggerDesc: 'Націсніце для сартавання па ўбыванні',
    triggerAsc: 'Націсніце для сартавання па ўзрастанні',
    cancelSort: 'Націсніце, каб адмяніць сартаванне'
  },
  Modal: {
    okText: 'OK',
    cancelText: 'Адмена',
    justOkText: 'OK'
  },
  Popconfirm: {
    okText: 'OK',
    cancelText: 'Адмена'
  },
  Transfer: {
    titles: ['', ''],
    searchPlaceholder: 'Пошук',
    itemUnit: 'элем.',
    itemsUnit: 'элем.',
    remove: 'Выдаліць',
    selectCurrent: 'Вылучыць бягучую старонку',
    removeCurrent: 'Выдаліць бягучую старонку',
    selectAll: 'Выбраць усе даныя',
    removeAll: 'Выдаліць усе даныя',
    selectInvert: 'Паказаць у адваротным парадку'
  },
  Upload: {
    uploading: 'Запампоўка...',
    removeFile: 'Выдаліць файл',
    uploadError: 'Адбылася памылка пры запампоўцы',
    previewFile: 'Перадпрагляд файла',
    downloadFile: 'Спампаваць файл'
  },
  Empty: {
    description: 'Няма даных'
  },
  Icon: {
    icon: 'Іконка'
  },
  Text: {
    edit: 'Рэдагаваць',
    copy: 'Капіяваць',
    copied: 'Капіяванне завершана',
    expand: 'Разгарнуць'
  },
  Form: {
    optional: '(не абавязкова)',
    defaultValidateMessages: {
      default: 'Памылка праверкі поля «${label}»',
      required: 'Калі ласка, увядзіце «${label}»',
      enum: 'Поле «${label}» павінна быць адным з [${enum}]',
      whitespace: 'Поле «${label}» не можа быць пустым',
      date: {
        format: 'Поле «${label}» мае няправільны фармат даты',
        parse: 'Поле «${label}» не можа быць пераўтворана ў дату',
        invalid: "Поле «${label}» не з'яўляецца карэктнай датай"
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
        len: 'Значэнне поля «${label}» павінна мець даўжыню ${len} сімвалаў',
        min: 'Значэнне поля «${label}» павінна мець не меней за ${min} сімвалаў',
        max: 'Значэнне поля «${label}» павінна быць не даўжэй за ${max} сімвалаў',
        range: 'Значэнне поля «${label}» павінна мець даўжыню ${min}-${max} сімвалаў'
      },
      number: {
        len: 'Значэнне поля «${label}» павінна быць роўнае ${len}',
        min: 'Значэнне поля «${label}» павінна быць больш або роўнае ${min}',
        max: 'Значэнне поля «${label}» павінна быць больш або роўнае ${max}',
        range: 'Значэнне поля «${label}» павінна быць паміж ${min} і ${max}'
      },
      array: {
        len: 'Колькасць элементаў у полі «${label}» павінна быць роўная ${len}',
        min: 'Колькасць элементаў у полі «${label}» павінна быць не меней за ${min}',
        max: 'Колькасць элементаў у полі «${label}» павінна быць не болей за ${max}',
        range: 'Колькасць элементаў у полі «${label}» павінна быць паміж ${min} і ${max}'
      },
      pattern: {
        mismatch: 'Значэнне поля «${label}» не адпавядае шаблону ${pattern}'
      }
    }
  },
  Image: {
    preview: 'Preview'
  }
};
var _default = exports.default = localeValues;