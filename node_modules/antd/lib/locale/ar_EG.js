"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _ar_EG = _interopRequireDefault(require("rc-pagination/lib/locale/ar_EG"));
var _ar_EG2 = _interopRequireDefault(require("../calendar/locale/ar_EG"));
var _ar_EG3 = _interopRequireDefault(require("../date-picker/locale/ar_EG"));
var _ar_EG4 = _interopRequireDefault(require("../time-picker/locale/ar_EG"));
/* eslint-disable no-template-curly-in-string */

const typeTemplate = 'ليس ${label} من نوع ${type} صالحًا';
const localeValues = {
  locale: 'ar',
  Pagination: _ar_EG.default,
  DatePicker: _ar_EG3.default,
  TimePicker: _ar_EG4.default,
  Calendar: _ar_EG2.default,
  global: {
    placeholder: 'يرجى التحديد'
  },
  Table: {
    filterTitle: 'الفلاتر',
    filterConfirm: 'تأكيد',
    filterReset: 'إعادة ضبط',
    selectAll: 'اختيار الكل',
    selectInvert: 'إلغاء الاختيار',
    selectionAll: 'حدد جميع البيانات',
    sortTitle: 'رتب',
    expand: 'توسيع الصف',
    collapse: 'طي الصف',
    triggerDesc: 'ترتيب تنازلي',
    triggerAsc: 'ترتيب تصاعدي',
    cancelSort: 'إلغاء الترتيب'
  },
  Modal: {
    okText: 'تأكيد',
    cancelText: 'إلغاء',
    justOkText: 'تأكيد'
  },
  Popconfirm: {
    okText: 'تأكيد',
    cancelText: 'إلغاء'
  },
  Transfer: {
    titles: ['', ''],
    searchPlaceholder: 'ابحث هنا',
    itemUnit: 'عنصر',
    itemsUnit: 'عناصر'
  },
  Upload: {
    uploading: 'جاري الرفع...',
    removeFile: 'احذف الملف',
    uploadError: 'مشكلة فى الرفع',
    previewFile: 'استعرض الملف',
    downloadFile: 'تحميل الملف'
  },
  Empty: {
    description: 'لا توجد بيانات'
  },
  Icon: {
    icon: 'أيقونة'
  },
  Text: {
    edit: 'تعديل',
    copy: 'نسخ',
    copied: 'نقل',
    expand: 'وسع'
  },
  Form: {
    defaultValidateMessages: {
      default: 'خطأ في حقل الإدخال ${label}',
      required: 'يرجى إدخال ${label}',
      enum: '${label} يجب أن يكون واحدا من [${enum}]',
      whitespace: '${label} لا يمكن أن يكون حرفًا فارغًا',
      date: {
        format: '${label} تنسيق التاريخ غير صحيح',
        parse: '${label} لا يمكن تحويلها إلى تاريخ',
        invalid: 'تاريخ ${label} غير صحيح'
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
        len: 'يجب ${label} ان يكون ${len} أحرف',
        min: '${label} على الأقل ${min} أحرف',
        max: '${label} يصل إلى ${max} أحرف',
        range: 'يجب ${label} ان يكون مابين ${min}-${max} أحرف'
      },
      number: {
        len: '${len} ان يساوي ${label} يجب',
        min: '${min} الأدنى هو ${label} حد',
        max: '${max} الأقصى هو ${label} حد',
        range: '${max}-${min} ان يكون مابين ${label} يجب'
      },
      array: {
        len: 'يجب أن يكون ${label} طوله ${len}',
        min: 'يجب أن يكون ${label} طوله الأدنى ${min}',
        max: 'يجب أن يكون ${label} طوله الأقصى ${max}',
        range: 'يجب أن يكون ${label} طوله مابين ${min}-${max}'
      },
      pattern: {
        mismatch: 'لا يتطابق ${label} مع ${pattern}'
      }
    }
  }
};
var _default = exports.default = localeValues;