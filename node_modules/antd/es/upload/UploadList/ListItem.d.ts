import * as React from 'react';
import type { ItemRender, UploadFile, UploadListProgressProps, UploadListType, UploadLocale } from '../interface';
export interface ListItemProps {
    prefixCls: string;
    className?: string;
    style?: React.CSSProperties;
    locale: UploadLocale;
    file: UploadFile;
    items: UploadFile[];
    listType?: UploadListType;
    isImgUrl?: (file: UploadFile) => boolean;
    showRemoveIcon?: boolean;
    showDownloadIcon?: boolean;
    showPreviewIcon?: boolean;
    removeIcon?: React.ReactNode | ((file: UploadFile) => React.ReactNode);
    downloadIcon?: React.ReactNode | ((file: UploadFile) => React.ReactNode);
    previewIcon?: React.ReactNode | ((file: UploadFile) => React.ReactNode);
    iconRender: (file: UploadFile) => React.ReactNode;
    actionIconRender: (customIcon: React.ReactNode, callback: () => void, prefixCls: string, title?: string, acceptUploadDisabled?: boolean) => React.ReactNode;
    itemRender?: ItemRender;
    onPreview: (file: UploadFile, e: React.SyntheticEvent<HTMLElement>) => void;
    onClose: (file: UploadFile) => void;
    onDownload: (file: UploadFile) => void;
    progress?: UploadListProgressProps;
}
declare const ListItem: React.ForwardRefExoticComponent<ListItemProps & React.RefAttributes<HTMLDivElement>>;
export default ListItem;
