import { lintWarning } from "./utils";
var linter = function linter(key, value, info) {
  if (typeof value === 'string' && /NaN/g.test(value) || Number.isNaN(value)) {
    lintWarning("Unexpected 'NaN' in property '".concat(key, ": ").concat(value, "'."), info);
  }
};
export default linter;