import type { FormInstance } from '../interface';
export declare function toArray<T>(value?: T | T[] | null): T[];
export declare function isFormInstance<T>(form: T | FormInstance): form is FormInstance;
