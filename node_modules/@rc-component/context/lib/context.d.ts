import * as React from 'react';
export type Selector<ContextProps, SelectorValue = ContextProps> = (value: ContextProps) => SelectorValue;
export type Trigger<ContextProps> = (value: ContextProps) => void;
export type Listeners<ContextProps> = Set<Trigger<ContextProps>>;
export interface Context<ContextProps> {
    getValue: () => ContextProps;
    listeners: Listeners<ContextProps>;
}
export interface ContextSelectorProviderProps<T> {
    value: T;
    children?: React.ReactNode;
}
export interface SelectorContext<ContextProps> {
    Context: React.Context<Context<ContextProps>>;
    Provider: React.ComponentType<ContextSelectorProviderProps<ContextProps>>;
    defaultValue?: ContextProps;
}
export declare function createContext<ContextProps>(defaultValue?: ContextProps): SelectorContext<ContextProps>;
/** e.g. useSelect(userContext) => user */
export declare function useContext<ContextProps>(holder: SelectorContext<ContextProps>): ContextProps;
/** e.g. useSelect(userContext, user => user.name) => user.name */
export declare function useContext<ContextProps, SelectorValue>(holder: SelectorContext<ContextProps>, selector: Selector<ContextProps, SelectorValue>): SelectorValue;
/** e.g. useSelect(userContext, ['name', 'age']) => user { name, age } */
export declare function useContext<ContextProps, SelectorValue extends Partial<ContextProps>>(holder: SelectorContext<ContextProps>, selector: (keyof ContextProps)[]): SelectorValue;
/** e.g. useSelect(userContext, 'name') => user.name */
export declare function useContext<ContextProps, PropName extends keyof ContextProps>(holder: SelectorContext<ContextProps>, selector: PropName): ContextProps[PropName];
