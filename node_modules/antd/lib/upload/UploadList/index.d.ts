import * as React from 'react';
import type { UploadFile, UploadListProps } from '../interface';
interface UploadListRef {
    handlePreview: (file: UploadFile, e?: React.SyntheticEvent<HTMLElement>) => void;
    handleDownload: (file: UploadFile) => void;
}
declare const UploadList: React.ForwardRefExoticComponent<UploadListProps<any> & React.RefAttributes<UploadListRef>>;
export default UploadList;
