declare function throttleByAnimationFrame<T extends any[]>(fn: (...args: T) => void): {
    (...args: T): void;
    cancel(): void;
};
export default throttleByAnimationFrame;
