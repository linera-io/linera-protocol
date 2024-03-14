export default function useShowNow(picker, mode, showNow, showToday, rangePicker) {
  if (mode !== 'date' && mode !== 'time') {
    return false;
  }
  if (showNow !== undefined) {
    return showNow;
  }

  // Compatible with old version `showToday`
  if (showToday !== undefined) {
    return showToday;
  }
  return !rangePicker && (picker === 'date' || picker === 'time');
}