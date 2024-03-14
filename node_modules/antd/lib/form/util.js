"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.getFieldId = getFieldId;
exports.getStatus = getStatus;
exports.toArray = toArray;
// form item name black list.  in form ,you can use form.id get the form item element.
// use object hasOwnProperty will get better performance if black list is longer.
const formItemNameBlackList = ['parentNode'];
// default form item id prefix.
const defaultItemNamePrefixCls = 'form_item';
function toArray(candidate) {
  if (candidate === undefined || candidate === false) return [];
  return Array.isArray(candidate) ? candidate : [candidate];
}
function getFieldId(namePath, formName) {
  if (!namePath.length) {
    return undefined;
  }
  const mergedId = namePath.join('_');
  if (formName) {
    return `${formName}_${mergedId}`;
  }
  const isIllegalName = formItemNameBlackList.includes(mergedId);
  return isIllegalName ? `${defaultItemNamePrefixCls}_${mergedId}` : mergedId;
}
/**
 * Get merged status by meta or passed `validateStatus`.
 */
function getStatus(errors, warnings, meta, defaultValidateStatus, hasFeedback, validateStatus) {
  let status = defaultValidateStatus;
  if (validateStatus !== undefined) {
    status = validateStatus;
  } else if (meta.validating) {
    status = 'validating';
  } else if (errors.length) {
    status = 'error';
  } else if (warnings.length) {
    status = 'warning';
  } else if (meta.touched || hasFeedback && meta.validated) {
    // success feedback should display when pass hasFeedback prop and current value is valid value
    status = 'success';
  }
  return status;
}