import * as React from 'react';
import type { ValidateStatus } from './FormItem';
export interface ErrorListProps {
    fieldId?: string;
    help?: React.ReactNode;
    helpStatus?: ValidateStatus;
    errors?: React.ReactNode[];
    warnings?: React.ReactNode[];
    className?: string;
    onVisibleChanged?: (visible: boolean) => void;
}
declare const ErrorList: React.FC<ErrorListProps>;
export default ErrorList;
