const isNumeric = value => !isNaN(parseFloat(value)) && isFinite(value);
export default isNumeric;