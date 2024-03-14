export function toArray(value) {
  if (value === undefined || value === null) {
    return [];
  }
  return Array.isArray(value) ? value : [value];
}
export function isFormInstance(form) {
  return form && !!form._init;
}