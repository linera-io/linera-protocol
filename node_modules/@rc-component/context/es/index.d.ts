/// <reference types="react" />
import type { SelectorContext } from './context';
import { createContext, useContext } from './context';
import createImmutable from './Immutable';
declare const makeImmutable: <T extends import("react").ComponentType<any>>(Component: T, shouldTriggerRender?: import("./Immutable").CompareProps<T>) => T, responseImmutable: <T extends import("react").ComponentType<any>>(Component: T, propsAreEqual?: import("./Immutable").CompareProps<T>) => T, useImmutableMark: () => number;
export { createContext, useContext, createImmutable, makeImmutable, responseImmutable, useImmutableMark, };
export type { SelectorContext };
