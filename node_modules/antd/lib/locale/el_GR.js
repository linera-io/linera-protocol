"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _el_GR = _interopRequireDefault(require("rc-pagination/lib/locale/el_GR"));
var _el_GR2 = _interopRequireDefault(require("../calendar/locale/el_GR"));
var _el_GR3 = _interopRequireDefault(require("../date-picker/locale/el_GR"));
var _el_GR4 = _interopRequireDefault(require("../time-picker/locale/el_GR"));
const localeValues = {
  locale: 'el',
  Pagination: _el_GR.default,
  DatePicker: _el_GR3.default,
  TimePicker: _el_GR4.default,
  Calendar: _el_GR2.default,
  Table: {
    filterTitle: 'Μενού φίλτρων',
    filterConfirm: 'ΟΚ',
    filterReset: 'Επαναφορά',
    selectAll: 'Επιλογή τρέχουσας σελίδας',
    selectInvert: 'Αντιστροφή τρέχουσας σελίδας'
  },
  Modal: {
    okText: 'ΟΚ',
    cancelText: 'Άκυρο',
    justOkText: 'ΟΚ'
  },
  Popconfirm: {
    okText: 'ΟΚ',
    cancelText: 'Άκυρο'
  },
  Transfer: {
    titles: ['', ''],
    searchPlaceholder: 'Αναζήτηση',
    itemUnit: 'αντικείμενο',
    itemsUnit: 'αντικείμενα'
  },
  Upload: {
    uploading: 'Μεταφόρτωση...',
    removeFile: 'Αφαίρεση αρχείου',
    uploadError: 'Σφάλμα μεταφόρτωσης',
    previewFile: 'Προεπισκόπηση αρχείου',
    downloadFile: 'Λήψη αρχείου'
  },
  Empty: {
    description: 'Δεν υπάρχουν δεδομένα'
  }
};
var _default = exports.default = localeValues;