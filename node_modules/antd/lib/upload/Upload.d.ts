import * as React from 'react';
import type { UploadProps as RcUploadProps } from 'rc-upload';
import RcUpload from 'rc-upload';
import type { RcFile, UploadFile, UploadProps } from './interface';
export declare const LIST_IGNORE: string;
export type { UploadProps };
export interface UploadRef<T = any> {
    onBatchStart: RcUploadProps['onBatchStart'];
    onSuccess: (response: any, file: RcFile, xhr: any) => void;
    onProgress: (e: {
        percent: number;
    }, file: RcFile) => void;
    onError: (error: Error, response: any, file: RcFile) => void;
    fileList: UploadFile<T>[];
    upload: RcUpload | null;
}
declare const Upload: React.ForwardRefExoticComponent<UploadProps<any> & React.RefAttributes<UploadRef<any>>>;
export default Upload;
