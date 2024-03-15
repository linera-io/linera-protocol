import React, { Component } from 'react';
import type { RcFile, UploadProps } from './interface';
interface ParsedFileInfo {
    origin: RcFile;
    action: string;
    data: Record<string, unknown>;
    parsedFile: RcFile;
}
declare class AjaxUploader extends Component<UploadProps> {
    state: {
        uid: string;
    };
    reqs: any;
    private fileInput;
    private _isMounted;
    onChange: (e: React.ChangeEvent<HTMLInputElement>) => void;
    onClick: (event: React.MouseEvent<HTMLDivElement> | React.KeyboardEvent<HTMLDivElement>) => void;
    onKeyDown: (e: React.KeyboardEvent<HTMLDivElement>) => void;
    onFileDrop: (e: React.DragEvent<HTMLDivElement>) => void;
    componentDidMount(): void;
    componentWillUnmount(): void;
    uploadFiles: (files: File[]) => void;
    /**
     * Process file before upload. When all the file is ready, we start upload.
     */
    processFile: (file: RcFile, fileList: RcFile[]) => Promise<ParsedFileInfo>;
    post({ data, origin, action, parsedFile }: ParsedFileInfo): void;
    reset(): void;
    abort(file?: any): void;
    saveFileInput: (node: HTMLInputElement) => void;
    render(): React.JSX.Element;
}
export default AjaxUploader;
