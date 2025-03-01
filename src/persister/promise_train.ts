export class PromiseTrain {
  private _promise: Promise<any> | undefined;

  async push(f: () => Promise<any>): Promise<void> {
    while (this._promise) {
      await this._promise;
    }
    this._promise = f().finally(() => (this._promise = undefined));
  }
}
