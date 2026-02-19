export class LockError extends Error {
  constructor(message: string, stack?: string) {
    super(message);
    if (stack) this.stack = stack;
  }
}
