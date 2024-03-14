"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _fi_FI = _interopRequireDefault(require("rc-pagination/lib/locale/fi_FI"));
var _fi_FI2 = _interopRequireDefault(require("../calendar/locale/fi_FI"));
var _fi_FI3 = _interopRequireDefault(require("../date-picker/locale/fi_FI"));
var _fi_FI4 = _interopRequireDefault(require("../time-picker/locale/fi_FI"));
const localeValues = {
  locale: 'fi',
  Pagination: _fi_FI.default,
  DatePicker: _fi_FI3.default,
  TimePicker: _fi_FI4.default,
  Calendar: _fi_FI2.default,
  Table: {
    filterTitle: 'Suodatus valikko',
    filterConfirm: 'OK',
    filterReset: 'Tyhjennä',
    selectAll: 'Valitse kaikki',
    selectInvert: 'Valitse päinvastoin',
    sortTitle: 'Lajittele',
    triggerDesc: 'Lajittele laskevasti',
    triggerAsc: 'Lajittele nousevasti',
    cancelSort: 'Peruuta lajittelu'
  },
  Modal: {
    okText: 'OK',
    cancelText: 'Peruuta',
    justOkText: 'OK'
  },
  Popconfirm: {
    okText: 'OK',
    cancelText: 'Peruuta'
  },
  Transfer: {
    titles: ['', ''],
    searchPlaceholder: 'Etsi täältä',
    itemUnit: 'kohde',
    itemsUnit: 'kohdetta'
  },
  Upload: {
    uploading: 'Lähetetään...',
    removeFile: 'Poista tiedosto',
    uploadError: 'Virhe lähetyksessä',
    previewFile: 'Esikatsele tiedostoa',
    downloadFile: 'Lataa tiedosto'
  },
  Empty: {
    description: 'Ei kohteita'
  },
  Text: {
    edit: 'Muokkaa',
    copy: 'Kopioi',
    copied: 'Kopioitu',
    expand: 'Näytä lisää'
  }
};
var _default = exports.default = localeValues;