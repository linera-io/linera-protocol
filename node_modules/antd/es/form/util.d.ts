/// <reference types="react" />
import type { Meta } from 'rc-field-form/lib/interface';
import type { ValidateStatus } from './FormItem';
import type { InternalNamePath } from './interface';
export declare function toArray<T>(candidate?: T | T[] | false): T[];
export declare function getFieldId(namePath: InternalNamePath, formName?: string): string | undefined;
/**
 * Get merged status by meta or passed `validateStatus`.
 */
export declare function getStatus<DefaultValue>(errors: React.ReactNode[], warnings: React.ReactNode[], meta: Meta, defaultValidateStatus: ValidateStatus | DefaultValue, hasFeedback?: boolean, validateStatus?: ValidateStatus): ValidateStatus | DefaultValue;
