/// <reference types="react" />
import Dragger from './Dragger';
import type { UploadProps } from './Upload';
import InternalUpload from './Upload';
export type { DraggerProps } from './Dragger';
export type { RcFile, UploadChangeParam, UploadFile, UploadListProps, UploadProps, } from './interface';
type InternalUploadType = typeof InternalUpload;
type CompoundedComponent<T = any> = InternalUploadType & {
    <U extends T>(props: React.PropsWithChildren<UploadProps<U>> & React.RefAttributes<any>): React.ReactElement;
    Dragger: typeof Dragger;
    LIST_IGNORE: string;
};
declare const Upload: CompoundedComponent<any>;
export default Upload;
