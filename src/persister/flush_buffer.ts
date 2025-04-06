import { strict as assert } from "assert";

export class FlushBuffer<T> {
  private _items: T[] = [];
  private _timer: NodeJS.Timeout | undefined;

  private _maxLength: number;
  private _flusher: (items: T[]) => Promise<void>;

  constructor(maxLength: number, flusher: (items: T[]) => Promise<void>) {
    this._maxLength = maxLength;
    this._flusher = flusher;
  }

  async push(t: T): Promise<void> {
    this._items.push(t);
    if (this._items.length >= this._maxLength) {
      await this._flush();
    } else {
      this._timer ??= setTimeout(() => void this._flush(), 0);
    }
  }

  private _flush(): Promise<void> {
    if (this._timer) {
      clearTimeout(this._timer);
      this._timer = undefined;
    }
    assert(this._items.length > 0);
    const items = this._items;
    this._items = [];
    return this._flusher(items);
  }

  abort(): void {
    if (this._timer) {
      clearTimeout(this._timer);
      this._timer = undefined;
    }
  }
}
