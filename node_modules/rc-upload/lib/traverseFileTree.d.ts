import type { RcFile } from './interface';
interface InternalDataTransferItem extends DataTransferItem {
    isFile: boolean;
    file: (cd: (file: RcFile & {
        webkitRelativePath?: string;
    }) => void) => void;
    createReader: () => any;
    fullPath: string;
    isDirectory: boolean;
    name: string;
    path: string;
}
declare const traverseFileTree: (files: InternalDataTransferItem[], callback: any, isAccepted: any) => void;
export default traverseFileTree;
