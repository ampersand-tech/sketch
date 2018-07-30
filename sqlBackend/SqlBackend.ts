/**
* Copyright 2018-present Ampersand Technologies, Inc.
*/

import { AccountID, BackingContext, SketchBackendInterface, UserInfo } from '../lib/index2';

import { wrap } from 'amper-utils/dist2017/promiseUtils';

export interface SqlInterface {
  setupTable(tableDesc, onCreateFunc?): SqlTable;

  allocShared(dimsHolder: ContextMetricDimensionsHolder|undefined, readOnly: boolean): SqlLink|undefined;
  allocTransacted(dimsHolder: ContextMetricDimensionsHolder|undefined, cb: ErrDataCB<SqlLink>): void;
  free(link: OptSqlLink, hadDomainError?: boolean): void;

  isTransactLink(link: OptSqlLink): boolean;
  transact(link: SqlLink, msg: string, cb: ErrDataCB<void>): void;
  commit(link: OptSqlLink, cb: ErrDataCB<void>): void;
  rollback(link: OptSqlLink, err: any, cb: ErrDataCB<void>): void;

  tableFind(link: OptSqlLink, table: SqlTable, query: Stash|undefined, opts: Stash|undefined, fields?: Stash): Promise<Stash[]>;
  tableUpdate(link: OptSqlLink, table: SqlTable, criteria: Stash, obj: Stash): Promise<number>;
  tableInsert(link: OptSqlLink, table: SqlTable, obj: Stash): Promise<Stash>;
  tableUpsert(
    link: OptSqlLink,
    table: SqlTable,
    criteria: Stash,
    obj: Stash,
  ): Promise<Stash>;
  tableRemove(link: OptSqlLink, table: SqlTable, criteria: Stash): Promise<number>;
  tableIncrementField(
    link: OptSqlLink,
    table: SqlTable,
    criteria: Stash,
    fieldAmount: Stash,
  ): Promise<Stash|undefined>;
}

let Sql: SqlInterface;

export class SketchSqlBackend extends SketchBackendInterface {
  async getUser(accountID: AccountID): Promise<UserInfo> {
    return {
      id: accountID,
      access: '',
      type: '',
      tableSubs: {},
    };
  }

  async startTransaction(ctx: BackingContext, name: string) {
    if (!ctx) {
      throw new Error('offline');
    }
    if (!ctx.sql) {
      throw new Error('offline');
    }

    if (ctx.sql.transact) {
      //Log.error(ctx, '@caller', 'startTransaction called inside a non-sketch transaction');
      throw new Error('internalServerError');
    }

    if (!Sql.isTransactLink(ctx.sql)) {
      ctx.sql = await wrap(Sql.allocTransacted, ctx);
    }

    await Sql.startTransaction(ctx.sql, name);
  }

  async commitTransaction(ctx: BackingContext) {
    if (!ctx) {
      throw new Error('offline');
    }
    try {
      await Sql.commitTransaction(ctx.sql);
    } finally {
      Sql.free(ctx.sql);
      ctx.sql = Sql.allocShared(ctx, ctx.readOnly || false);
    }
  }

  async rollbackTransaction(err: any, ctx: BackingContext) {
    if (!ctx) {
      throw new Error('offline');
    }
    try {
      await Sql.rollbackTransaction(ctx.sql, err);
    } finally {
      Sql.free(ctx.sql);
      ctx.sql = Sql.allocShared(ctx, ctx.readOnly || false);
    }
  }
}
