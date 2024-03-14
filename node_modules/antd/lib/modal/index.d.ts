import type { ModalStaticFunctions } from './confirm';
import { modalGlobalConfig } from './confirm';
import OriginModal from './Modal';
import PurePanel from './PurePanel';
import useModal from './useModal';
export type { ModalFuncProps, ModalLocale, ModalProps } from './interface';
type ModalType = typeof OriginModal & ModalStaticFunctions & {
    useModal: typeof useModal;
    destroyAll: () => void;
    config: typeof modalGlobalConfig;
    /** @private Internal Component. Do not use in your production. */
    _InternalPanelDoNotUseOrYouWillBeFired: typeof PurePanel;
};
declare const Modal: ModalType;
export default Modal;
