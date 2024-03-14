import * as React from 'react';
import type { ModalFunc, ModalStaticFunctions } from '../confirm';
export type ModalFuncWithPromise = (...args: Parameters<ModalFunc>) => ReturnType<ModalFunc> & {
    then<T>(resolve: (confirmed: boolean) => T, reject: VoidFunction): Promise<T>;
};
export type HookAPI = Omit<Record<keyof ModalStaticFunctions, ModalFuncWithPromise>, 'warn'>;
declare function useModal(): readonly [instance: HookAPI, contextHolder: React.ReactElement];
export default useModal;
