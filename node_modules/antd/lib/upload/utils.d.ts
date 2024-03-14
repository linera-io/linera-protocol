import type { InternalUploadFile, RcFile, UploadFile } from './interface';
export declare function file2Obj(file: RcFile): InternalUploadFile;
/** Upload fileList. Replace file if exist or just push into it. */
export declare function updateFileList(file: UploadFile, fileList: (UploadFile | Readonly<UploadFile>)[]): (UploadFile<any> | Readonly<UploadFile<any>>)[];
export declare function getFileItem(file: RcFile, fileList: (UploadFile | Readonly<UploadFile>)[]): UploadFile<any> | Readonly<UploadFile<any>>;
export declare function removeFileItem(file: UploadFile, fileList: (UploadFile | Readonly<UploadFile>)[]): (UploadFile<any> | Readonly<UploadFile<any>>)[] | null;
export declare const isImageUrl: (file: UploadFile) => boolean;
export declare function previewImage(file: File | Blob): Promise<string>;
