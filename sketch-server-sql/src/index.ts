/**
* Copyright 2018-present Ampersand Technologies, Inc.
*/

import { AccountID, BackingContext, SketchBackendInterface, UserInfo } from '../lib/index';

import { wrap } from 'amper-utils/dist/promiseUtils';
import { Stash } from 'amper-utils/dist/types';

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
  async init() {
    // TODO setup tables
  }

  async getUser(accountID: AccountID): Promise<UserInfo> {
    return await wrap(AccountTokenCache.findByAccountID, ctx, accountID, 'user');
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

  async mergeAndWriteFeed(ctx: BackingContext, feedEntries: Stash[]) {
    const feedToWrite = Feed.mergeFeedEntries(feedEntries);

    for (let i = 0; i < feedToWrite.length; ++i) {
      const feedData = feedToWrite[i];
      await wrap(Feed.addMulti, ctx, feedData.accountIDs, feedData.keys, feedData.fields, feedData.clientKey);
    }
  }
}
