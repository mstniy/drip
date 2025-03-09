export class PromiseTrain {
  private _promise: Promise<unknown> | undefined;

  async push(f: () => Promise<unknown>): Promise<void> {
    while (this._promise) {
      await this._promise;
    }
    this._promise = f().finally(() => (this._promise = undefined));
  }
}
