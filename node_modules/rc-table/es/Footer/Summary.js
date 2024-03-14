import Cell from "./Cell";
import Row from "./Row";
/**
 * Syntactic sugar. Do not support HOC.
 */
function Summary(_ref) {
  var children = _ref.children;
  return children;
}
Summary.Row = Row;
Summary.Cell = Cell;
export default Summary;