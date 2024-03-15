import type { UploadRequestOption } from './interface';
export default function upload(option: UploadRequestOption): {
    abort(): void;
};
