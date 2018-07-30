/**
* Copyright 2018-present Ampersand Technologies, Inc.
*/

import { Stash } from 'amper-utils/dist2017/types';

export type SketchWildcard = '*';
export type SketchMask = number | SketchWildcard | { [k: string]: SketchMask };
export type OptMask = SketchMask | null;

interface _AccountIDTag { ___AccountID: undefined; }
export type AccountID = _AccountIDTag & string;


export type BackingContext = Stash|null;

export interface UserInfo {
  id: AccountID;
  access: string;
  type: string;
  tableSubs: Stash;
}

export abstract class SketchBackendInterface {
  abstract async getUser(accountID: AccountID): Promise<UserInfo>;
  abstract async startTransaction(ctx: BackingContext, name: string);
  abstract async commitTransaction(ctx: BackingContext);
  abstract async rollbackTransaction(err: any, ctx: BackingContext);
}

class SketchBaseContext {
  public user: UserInfo = {
    id: '' as AccountID,
    access: '',
    type: '',
    tableSubs: {},
  };

  private readOnly = false;
  private activeTransactions: string[] = [];
  private feedToWrite: undefined | any[];
  rowLocks: undefined | Stash;

  constructor(
    protected readonly backend: SketchBackendInterface,
    protected readonly backingContext: BackingContext,
    protected readonly parentContext: SketchContext | undefined,
  ) {
    if (this.parentContext && this.parentContext.readOnly) {
      this.readOnly = true;
    }
    if (this.backingContext && this.backingContext.user) {
      this.user = this.backingContext.user;
    }
  }

  private async startTransaction(name: string) {
    if (this.readOnly) {
      //Log.error(this, '@caller', 'Sketch: startTransaction called in a read-only context');
      throw new Error('internalServerError');
    }

    const isInTransaction = this.activeTransactions.length > 0;
    this.activeTransactions.push(name);

    if (isInTransaction) {
      return;
    }

    if (this instanceof SketchActionContext) {
      throw new Error('internalServerError');
    }

    this.feedToWrite = [];

    // used to track what rows have been locked with lockMembership() during this transaction
    this.rowLocks = {};

    await this.backend.startTransaction(this.backingContext, name);
  }

  private async endTransaction(name: string, err: any) {
    if (this.activeTransactions.length === 0) {
      //Log.error(this, '@caller', 'Sketch: endTransaction called outside a transaction');
      throw new Error('internalServerError');
    }

    if (this.activeTransactions[this.activeTransactions.length - 1] !== name) {
      //Log.error(this, '@caller', 'Sketch: endTransaction called outside a transaction');
      throw new Error('internalServerError');
    }

    this.activeTransactions.pop();
    if (this.activeTransactions.length) {
      // still inside a transaction
      return;
    }

    this.rowLocks = undefined;

    if (err) {
      this.feedToWrite = undefined;
      await this.backend.rollbackTransaction(err, this.backingContext);
      return;
    }

    await this.backend.commitTransaction(this.backingContext);
    await SketchAction.mergeAndWriteFeed(this.feedToWrite);
  }

  async runInTransaction<T>(name: string, func: (ctx: SketchBaseContext) => Promise<T>): Promise<T> {
    await this.startTransaction(name);

    let res: T|undefined;
    let err: any;
    try {
      res = await func(this);
    } catch (err_) {
      err = err_;
    }

    await this.endTransaction(name, err);
    if (err) {
      throw err;
    }
    return res!;
  }

  async getData(path: string[], mask?: SketchMask) {
  }

  async createData(path: string[], overrideFields?: Stash) {
  }

  async initializeData(path: string[], initPath = false) {
  }

  async updateData(path: string[], value: Stash|string|number|null) {
  }

  async replaceData(path: string[], value: Stash|string|number|null) {
  }

  async removeData(path: string[]) {
  }

  async addMember(path: string[], accountID: AccountID, overrideFields?: Stash) {
  }

  async removeMember(path: string[], accountID: AccountID) {
  }
}

class SketchContext extends SketchBaseContext {
  constructor(backend: SketchBackendInterface, backingContext: BackingContext, parentContext: SketchContext|undefined) {
    super(backend, backingContext, parentContext);
  }

  async asUser(accountID: AccountID) {
    const ctx = new SketchContext(this.backend, this.backingContext, this);
    ctx.user = await this.backend.getUser(accountID);
    return ctx;
  }

  async asAdmin() {
    return new SketchContext(this.backend, this.backingContext, this);
  }
}

class SketchActionContext extends SketchBaseContext {
  constructor(backend: SketchBackendInterface, backingContext: BackingContext) {
    super(backend, backingContext, undefined);
  }

  genUUID(name: string, optType?: string): string {
  }

  clientTime(): number {
  }

  serverTime(): number {
    return Date.now();
  }
}

export class Sketch {
  constructor(private readonly backend: SketchBackendInterface) {
  }

  async init() {
  }

  async asUser(backingContext: BackingContext, accountID: AccountID) {
    const ctx = new SketchContext(this.backend, backingContext, undefined);
    ctx.user = await this.backend.getUser(accountID);
    return ctx;
  }

  async asAdmin(backingContext: BackingContext) {
    return new SketchContext(this.backend, backingContext, undefined);
  }

  async transact<T>(backingContext: BackingContext, name: string, func: (ctx: SketchBaseContext) => Promise<T>) {
    const ctx = new SketchContext(this.backend, backingContext, undefined);
    return await ctx.runInTransaction(name, func);
  }

  async action<T>(backingContext: BackingContext, accountID: AccountID, name: string, func: (ctx: SketchBaseContext) => Promise<T>) {
    const ctx = new SketchActionContext(this.backend, backingContext);
    ctx.user = await this.backend.getUser(accountID);
    return await ctx.runInTransaction(name, func);
  }
}
