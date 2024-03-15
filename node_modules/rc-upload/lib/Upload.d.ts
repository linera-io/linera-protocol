import React, { Component } from 'react';
import AjaxUpload from './AjaxUploader';
import type { UploadProps, RcFile } from './interface';
declare function empty(): void;
declare class Upload extends Component<UploadProps> {
    static defaultProps: {
        component: string;
        prefixCls: string;
        data: {};
        headers: {};
        name: string;
        multipart: boolean;
        onStart: typeof empty;
        onError: typeof empty;
        onSuccess: typeof empty;
        multiple: boolean;
        beforeUpload: any;
        customRequest: any;
        withCredentials: boolean;
        openFileDialogOnClick: boolean;
        hasControlInside: boolean;
    };
    private uploader;
    abort(file: RcFile): void;
    saveUploader: (node: AjaxUpload) => void;
    render(): React.JSX.Element;
}
export default Upload;
