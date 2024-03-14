import type { ModalFuncProps } from './interface';
type ConfigUpdate = ModalFuncProps | ((prevConfig: ModalFuncProps) => ModalFuncProps);
export type ModalFunc = (props: ModalFuncProps) => {
    destroy: () => void;
    update: (configUpdate: ConfigUpdate) => void;
};
export type ModalStaticFunctions = Record<NonNullable<ModalFuncProps['type']>, ModalFunc>;
export default function confirm(config: ModalFuncProps): {
    destroy: (...args: any[]) => void;
    update: (configUpdate: ConfigUpdate) => void;
};
export declare function withWarn(props: ModalFuncProps): ModalFuncProps;
export declare function withInfo(props: ModalFuncProps): ModalFuncProps;
export declare function withSuccess(props: ModalFuncProps): ModalFuncProps;
export declare function withError(props: ModalFuncProps): ModalFuncProps;
export declare function withConfirm(props: ModalFuncProps): ModalFuncProps;
export declare function modalGlobalConfig({ rootPrefixCls }: {
    rootPrefixCls: string;
}): void;
export {};
