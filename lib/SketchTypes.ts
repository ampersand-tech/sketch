/**
* Copyright 2017-present Ampersand Technologies, Inc.
*/

import { Type, Schema } from './types';

import { Stash, StashOf } from 'amper-utils/dist2017/types';

export interface ActionPayload {
  argMap: Stash;
  clientKey: string;
  clientUUIDs: StashOf<string>;
  clientTime?: number;
  serverTime?: number;
}

export type SketchWildcard = '*';
export type SketchMask = number | SketchWildcard | { [k: string]: SketchMask };
export type OptMask = SketchMask | null;

interface SketchSharedContext {
  sketchActionData?: ActionPayload;
  sketchServerAction?: boolean;
  sketchServerActionData?: Stash;
  sketchTransactions?: any[];
  sketchCanParallelFetch?: boolean;
  sketchNoWatchTrigger?: boolean;
  sketchIsReplay?: boolean;
}

export interface SketchContext extends Context, SketchSharedContext {
}

export interface SketchServerContext extends ServerContext, SketchSharedContext {
  sketchServerActionName?: string;
  sketchFeedToWrite?: any[];
  sketchLocks?: Stash;
}

export interface AccountTokenCacheInterface {
  findByAccountID: (ctx: ServerContext, accountID: string, key: string, cb: ErrDataCB<any>) => void;
  resetAccountCache: (accountID: string) => void;
}

export interface IndexParams {
  primary?: boolean;
  unique?: boolean;
}

// Really Sql.Table from overlib/server/sql.ts
export interface SqlTable {
  name: string;
  primaryKeys: string[];
  isParallelStore: boolean;
  schema: Schema;
  indexes: [StashOf<1>, IndexParams][];
}

export interface TableOptions {
  windowOnly?: boolean;
  clientOnly?: boolean;
  serverOnly?: boolean;
  allowUnverifiedMembership?: boolean;
  perms?: PermFunc[];
  idType?: Type;
}

export interface SchemaRootMap {
  _nullable?: boolean;
  _tableOptions?: TableOptions;
  _ids: SchemaRootInternal;
}

export interface SchemaRootInternal {
  _nullable?: boolean;
  _tableOptions?: TableOptions;
  [subType: string]: (Schema | undefined | boolean | TableOptions);
}

export type SchemaRoot = SchemaRootMap | SchemaRootInternal;

export function isSchemaMapNode(root: SchemaRoot): root is SchemaRootMap {
  return root.hasOwnProperty('_ids');
}
