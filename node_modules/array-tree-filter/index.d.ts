declare function arrayTreeFilter<T>(data: T[], filterFn: (item: T, level: number) => boolean, options?: {
    childrenKeyName?: string;
}): T[];
export default arrayTreeFilter;
