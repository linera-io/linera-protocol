import { createContext, useContext } from "./context";
import createImmutable from "./Immutable";

// For legacy usage, we export it directly
var _createImmutable = createImmutable(),
  makeImmutable = _createImmutable.makeImmutable,
  responseImmutable = _createImmutable.responseImmutable,
  useImmutableMark = _createImmutable.useImmutableMark;
export { createContext, useContext, createImmutable, makeImmutable, responseImmutable, useImmutableMark };