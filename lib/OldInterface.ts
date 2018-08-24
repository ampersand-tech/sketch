/**
* Copyright 2015-present Ampersand Technologies, Inc.
*/

import * as Actions from './Actions';
import * as DataHelpers from './DataHelpers';
import * as DataReader from './DataReader';
import * as DataWriter from './DataWriter';
import * as SketchProgram from './SketchProgram';
import { AccountTokenCacheInterface, OptMask, SketchServerContext } from './SketchTypes';
import * as TableDef from './TableDef';
import * as Types from './types';

import * as ErrorUtils from 'amper-utils/dist2017/errorUtils';
import * as ObjUtils from 'amper-utils/dist2017/objUtils';
import { ignoreError } from 'amper-utils/dist2017/promiseUtils';
import { Stash } from 'amper-utils/dist2017/types';


let DataStore;
let Sql: SqlInterface;

export const ACCOUNT_MASK = ObjUtils.objectMakeImmutable({
  id: 1,
  access: 1,
  userType: 1,
  name: 1,
  email: 1,
});


let gIsServer = false;
let gIsClient = false;

let gClientUser;


type ModifyMethod = 'insert' | 'update' | 'upsert' | 'increment';

export function initServer(
  sqlModule: SqlInterface,
  feedModule,
  accountTokenCacheModule: AccountTokenCacheInterface | undefined,
) {
  gIsServer = true;
  Sql = sqlModule;
  Actions.initServer(feedModule);
  DataReader.init(null, sqlModule);
  DataWriter.init(feedModule, null, sqlModule);
  SketchProgram.initServer(accountTokenCacheModule);
  TableDef.initSqlTables(sqlModule.setupTable);
}

export function initClient(dataStoreModule, dbUtilModule) {
  gIsClient = true;
  DataStore = dataStoreModule;
  Actions.initClient(dataStoreModule, dbUtilModule);
  DataReader.init(dataStoreModule, undefined);
  DataWriter.init(null, dataStoreModule, undefined);
}


function validateCall(ctx: OptContext | ServerContext, tableName: string|null, isModify: boolean): SketchServerContext {
  if (!ctx) {
    if (!gIsClient) {
      throw new Error('Sketch: bad context passed to function');
    }

    if (!gClientUser && DataStore.hasDataStore('account')) {
      gClientUser = DataStore.getData(null, ['account'], ACCOUNT_MASK);
      if (!gClientUser.id) {
        gClientUser = null;
      }
    }
    ctx = {
      sql: undefined,
      user: gClientUser || {id: 'dummy'},
      dims: {},
    };
  }

  const retCtx = ctx as SketchServerContext;

  // verify this call is allowed on the given table in this context
  const isAction = !!retCtx.sketchActionData;
  const isServerAction = !!retCtx.sketchServerAction;
  const table = tableName ? TableDef.getTable(tableName) : null;
  if (table) {
    if (table.isSynced) {
      if (isModify && !isAction && gIsClient) {
        throw new Error('Sketch: attempt to modify a synced table on the client outside an action');
      }
    } else {
      if (gIsClient && !table.isClientVisible) {
        throw new Error('Sketch: attempt to access a server-only table on the client');
      }
      if (gIsServer && !table.isServerVisible) {
        throw new Error('Sketch: attempt to access a client-only table on the server');
      }
      if (isAction && !isServerAction) {
        throw new Error('Sketch: attempt to access an unsynced table inside an action');
      }
    }
  }

  return retCtx;
}

function validatePath(path: string[]) {
  if (!Array.isArray(path)) {
    throw new Error('non-array path found');
  }

  for (let i = 0; i < path.length; ++i) {
    if (!DataHelpers.isValidKey(path[i])) {
      throw new Error('invalid key "' + path[i] + '" found in path "' + path.join('/') + '"');
    }
  }
}

///////////////////////////////////////////////////////////////////////
// Client data manipulation
///////////////////////////////////////////////////////////////////////

export function clientDataForCreate(ctx: OptContext, path: string[]): any {
  const sctx = validateCall(ctx, path[0], false);
  validatePath(path);
  return DataHelpers.clientDataForCreate(sctx, path);
}

export async function clientDataExists(ctx: OptContext, path: string[]) {
  const res = await ignoreError(getClientData(ctx, path, 1), 'not found');
  return res !== undefined;
}

export async function serverDataExists(ctx: SketchServerContext, path: string[]) {
  const res = await ignoreError(getServerData(ctx, path, 1), 'not found');
  return res !== undefined;
}

export async function getClientData(ctx: OptContext, path: string[], fieldMask: OptMask) {
  const sctx = validateCall(ctx, path[0], false);
  validatePath(path);
  if (!sctx.user.id) {
    throw new Error('must be logged in');
  }
  if (gIsServer || gIsClient) {
    return await DataReader.getDataForFields(
      sctx,
      sctx.user.id,
      path,
      {
        allowServerData: false,
        allowMemberData: true,
        recursiveFetch:
        !fieldMask,
        fieldMask: fieldMask,
      },
    );
  }
  throw new Error('Sketch is not initialized');
}

async function getClientDataForSubscription(sctx: SketchServerContext, path: string[], key: string, dbOut: Stash) {
  const startTime = sctx.user.tableSubs[key].startTime;

  // TODO time filtering
  startTime;

  const data = await ignoreError(getClientData(sctx, path, null), 'not found');
  if (data !== undefined) {
    dbOut[key] = data;
  }
}

export async function getAndLockClientData(ctx: ServerContext, path: string[], fieldMask: OptMask) {
  const sctx = validateCall(ctx, path[0], false);
  validatePath(path);
  if (!sctx.user.id) {
    throw new Error('must be logged in');
  }
  if (!gIsServer) {
    throw new Error('getAndLockClientData can only run on the server');
  }
  if (!sctx.sketchTransactions || !sctx.sketchTransactions.length) {
    throw new Error('getAndLockClientData must be called from inside a transaction');
  }
  await DataReader.getDataForFields(
    sctx,
    sctx.user.id,
    path,
    {
      allowServerData: false,
      allowMemberData: true,
      recursiveFetch: !fieldMask,
      fieldMask: fieldMask,
      sqlOpts: {
        forUpdate: 1,
      },
    },
  );
}


export async function getClientTable(ctx: OptContext, tableName: string) {
  const sctx = validateCall(ctx, tableName, false);
  if (!sctx.user.id) {
    throw new Error('must be logged in');
  }
  if (gIsServer) {
    const sketchTable = TableDef.getTable(tableName, true);
    const data = await ignoreError(getClientData(sctx, [tableName], null), 'not found');
    if (data === undefined) {
      if (sketchTable.isRootMap) {
        return {};
      } else {
        return DataHelpers.clientDataForCreate(ctx, [sketchTable.name]);
      }
    }
    return data;
  }
  if (gIsClient) {
    return DataStore.getDataUnsafe([tableName]);
  }
  throw new Error('Sketch is not initialized');
}

// insert/update/upsert
async function modifyClientData(modifyMethod: ModifyMethod, ctx: OptContext, path: string[], data: any) {
  const sctx = validateCall(ctx, path[0], true);
  validatePath(path);
  if (!sctx.user.id) {
    throw new Error('must be logged in');
  }
  if (gIsServer || gIsClient) {
    return await DataWriter.modifyData(sctx, modifyMethod, sctx.user.id, path, data, { allowServerData: true, allowMemberData: true });
  }
  throw new Error('Sketch is not initialized');
}

export async function insertClientData(ctx: OptContext, path: string[], data: any) {
  return modifyClientData('insert', ctx, path, data);
}

export async function updateClientData(ctx: OptContext, path: string[], data: any) {
  return modifyClientData('update', ctx, path, data);
}


type Submap = {
  subpath: string[];
  data: Stash;
  schema: Types.SchemaMapNode;
};
function extractUpdateAndSubmaps(update: Stash, submaps: Submap[], schema: Types.Schema, data: Stash, subpath: string[]) {
  for (const key in schema) {
    if (Types.isSchemaMapNode(schema[key])) {
      // submap
      submaps.push({
        subpath: subpath.concat(key),
        data: data[key] || {},
        schema: schema[key],
      });
      delete update[key];
    } else if (!Types.isType(schema[key])) {
      // subobject
      extractUpdateAndSubmaps(update[key], submaps, schema[key], data[key] || {}, subpath.concat(key));
    } else if (data.hasOwnProperty(key)) {
      update[key] = data[key];
    }
  }
}

interface ReplaceFuncs {
  getData: (ctx: OptContext, path: string[], fieldMask: OptMask) => Promise<any>;
  modifyData: (method: ModifyMethod, ctx: OptContext, path: string[], data: any) => Promise<void>;
  removeData: (ctx: OptContext, path: string[]) => Promise<void>;
  dataForCreate: (ctx: OptContext, path: string[]) => Stash;
}
async function replaceDataInternal(ctx: OptContext, funcs: ReplaceFuncs, path: string[], data: any, schema: Types.Schema) {
  if (Types.isType(schema)) {
    // leaf field
    return funcs.modifyData('update', ctx, path, data);
  }

  if (Types.isSchemaMapNode(schema)) {
    return replaceSubMapInternal(ctx, funcs, path, data, schema);
  }

  const update = funcs.dataForCreate(ctx, path);
  const submaps: Submap[] = [];
  extractUpdateAndSubmaps(update, submaps, schema, data, []);

  await funcs.modifyData('update', ctx, path, update);

  for (let i = 0; i < submaps.length; ++i) {
    const submapInfo = submaps[i];
    await replaceSubMapInternal(ctx, funcs, path.concat(submapInfo.subpath), submapInfo.data, submapInfo.schema);
  }
}

async function replaceSubMapInternal(ctx: OptContext, funcs: ReplaceFuncs, path: string[], data: any, schema: Types.SchemaMapNode) {
  const existingIDs = await funcs.getData(ctx, path, Util.IDS_MASK);

  for (let key in existingIDs) {
    if (data.hasOwnProperty(key)) {
      // recursively replace
      await replaceDataInternal(ctx, funcs, path.concat(key), data[key], schema._ids);
    } else {
      await funcs.removeData(ctx, path.concat(key));
    }
  }

  for (let key in data) {
    if (!existingIDs.hasOwnProperty(key)) {
      await funcs.modifyData('insert', ctx, path.concat(key), data[key]);
    }
  }
}

const clientFuncs: ReplaceFuncs = {
  getData: getClientData,
  modifyData: modifyClientData,
  removeData: removeClientData,
  dataForCreate: DataHelpers.clientDataForCreate,
};

export async function replaceClientData(ctx: OptContext, path: string[], data: any) {
  const sctx = validateCall(ctx, path[0], true);
  validatePath(path);
  if (!sctx.user.id) {
    throw new Error('must be logged in');
  }
  const table = TableDef.getTable(path[0], false);
  if (!table) {
    throw new Error('bad path');
  }
  const schema = table.getServerSchema(path.slice(1));
  if (!schema) {
    throw new Error('bad path');
  }

  return await replaceDataInternal(sctx, clientFuncs, path, data, schema);
}

async function removeClientDataEx(ctx: OptContext, path: string[], errorOnNotFound: boolean) {
  const sctx = validateCall(ctx, path[0], true);
  if (!validatePath(path)) {
    return;
  }
  if (!sctx.user.id) {
    throw new Error('must be logged in');
  }
  if (gIsServer || gIsClient) {
    const fieldFilter = {
      allowServerData: true,
      allowMemberData: true,
      recursiveFetch: true,
    };
    let p = DataWriter.removeData(sctx, sctx.user.id, path, fieldFilter);
    if (!errorOnNotFound) {
      p = ignoreError(p, 'not found');
    }
    return await p;
  }
  throw new Error('Sketch is not initialized');
}

export async function removeClientData(ctx: OptContext, path: string[]) {
  await removeClientDataEx(ctx, path, true);
}

export async function removeClientDataIfExists(ctx: OptContext, path: string[]) {
  await removeClientDataEx(ctx, path, false);
}

// inserts default values if the path does not exist
export async function initializeClientData(ctx: OptContext, path: string[]) {
  try {
    getClientData(ctx, path, {});
  } catch (err) {
    if (ErrorUtils.isNotFound(err)) {
      await modifyClientData('insert', ctx, path, clientDataForCreate(ctx, path));
    } else {
      throw err;
    }
  }
}

export async function initializeClientPath(ctx: OptContext, path: string[]) {
  const sctx = validateCall(ctx, path[0], true);
  await initializePath(sctx, path, initializeClientData);
}

async function initializePath(sctx: SketchServerContext, path: string[], func) {
  validatePath(path);
  const table = TableDef.getTable(path[0], false);
  if (!table) {
    throw new Error('bad path');
  }

  let schema = table.dataSchema;
  for (let i = 1; i < path.length; ++i) {
    const p = path[i];
    if (schema[p]) {
      schema = schema[p];
    } else {
      const schemaKeys = Object.keys(schema);
      if (schemaKeys.length !== 1 || schemaKeys[0][0] !== '$') {
        throw new Error('path not in the table schema');
      }
      await func(sctx, path.slice(0, i + 1));
      schema = schema[schemaKeys[0]];
    }
  }
}


///////////////////////////////////////////////////////////////////////
// Server data manipulation (server-only functions)
///////////////////////////////////////////////////////////////////////

export async function getServerData(ctx: ServerContext, path: string[], fieldMask: OptMask) {
  const sctx = validateCall(ctx, path[0], false);
  validatePath(path);
  if (!gIsServer) {
    throw new Error('getServerData can only run on the server');
  }
  return await DataReader.getDataForFields(sctx, undefined, path, { allowServerData: true, allowMemberData: false, fieldMask: fieldMask });
}

export async function getAndLockServerData(ctx: ServerContext, path: string[], fieldMask: OptMask) {
  const sctx = validateCall(ctx, path[0], false);
  validatePath(path);
  if (!gIsServer) {
    throw new Error('getAndLockServerData can only run on the server');
  }
  if (!sctx.sketchTransactions || !sctx.sketchTransactions.length) {
    throw new Error('getAndLockServerData must be called from inside a transaction');
  }
  return await DataReader.getDataForFields(
    sctx,
    undefined,
    path,
    {
      allowServerData: true,
      allowMemberData: false,
      fieldMask: fieldMask,
      sqlOpts: {
        forUpdate: 1,
      },
    },
  );
}

// insert/update/upsert
async function modifyServerData(modifyMethod: ModifyMethod, ctx: ServerContext, path: string[], data: any) {
  const sctx = validateCall(ctx, path[0], true);
  if (!validatePath(path)) {
    return;
  }
  if (!gIsServer) {
    throw new Error('modifyServerData can only run on the server');
  }
  return await DataWriter.modifyData(sctx, modifyMethod, undefined, path, data, { allowServerData: true, allowMemberData: false });
}

export async function insertServerData(ctx: ServerContext, path: string[], data: any) {
  await modifyServerData('insert', ctx, path, data);
}

export async function updateServerData(ctx: ServerContext, path: string[], data: any) {
  await modifyServerData('update', ctx, path, data);
}

export async function incrementServerField(ctx: ServerContext, path: string[], incrementAmount: number) {
  const sctx = validateCall(ctx, path[0], true);
  validatePath(path);
  if (gIsServer) {
    return await modifyServerData('increment', sctx, path, incrementAmount);
  }
  if (gIsClient) {
    throw new Error('incrementServerField can only run on the server');
  }
  throw new Error('Sketch is not initialized');
}

const serverFuncs = {
  getData: getServerData,
  modifyData: modifyServerData,
  removeData: removeServerData,
  dataForCreate: DataHelpers.serverDataForCreate,
};

export async function replaceServerData(ctx: ServerContext, path: string[], data: any) {
  const sctx = validateCall(ctx, path[0], true);
  validatePath(path);
  const table = TableDef.getTable(path[0], false);
  if (!table) {
    throw new Error('bad path');
  }
  const schema = table.getServerSchema(path.slice(1));
  if (!schema) {
    throw new Error('bad path');
  }

  await replaceDataInternal(sctx, serverFuncs, path, data, schema);
}

async function removeServerDataEx(ctx: ServerContext, path: string[], errorOnNotFound: boolean) {
  const sctx = validateCall(ctx, path[0], true);
  validatePath(path);
  if (!gIsServer) {
    throw new Error('removeServerData can only run on the server');
  }
  const fieldFilter = {
    allowServerData: true,
    allowMemberData: true,
    recursiveFetch: true,
  };
  let p = DataWriter.removeData(sctx, undefined, path, fieldFilter);
  if (!errorOnNotFound) {
    p = ignoreError(p, 'not found');
  }
  await p;
}

export async function removeServerData(ctx: ServerContext, path: string[]) {
  await removeServerDataEx(ctx, path, true);
}

export async function removeServerDataIfExists(ctx: ServerContext, path: string[]) {
  await removeServerDataEx(ctx, path, false);
}

// inserts default values if the path does not exist
export async function initializeServerData(ctx: ServerContext, path: string[]) {
  try {
    getServerData(ctx, path, {});
  } catch (err) {
    if (ErrorUtils.isNotFound(err)) {
      await modifyServerData('insert', ctx, path, DataHelpers.serverDataForCreate(ctx, path));
    } else {
      throw err;
    }
  }
}

export async function initializeServerPath(ctx: OptContext, path: string[]) {
  const sctx = validateCall(ctx, path[0], true);
  await initializePath(sctx, path, initializeServerData);
}


///////////////////////////////////////////////////////////////////////
// Membership
///////////////////////////////////////////////////////////////////////

export async function isMember(ctx: ServerContext, path: string[], accountID: AccountID) {
  const sctx = validateCall(ctx, path[0], false);
  validatePath(path);
  if (path.length !== 2) {
    throw new Error('isMember can only be called at the root object level');
  }
  if (gIsServer) {
    return await DataReader.isMember(sctx, path, accountID);
  }
  if (gIsClient) {
    throw new Error('isMember can only run on the server');
  }
  throw new Error('Sketch is not initialized');
}

export async function findMember(ctx: ServerContext, path: string[], searchFields: Stash, fieldMask: OptMask) {
  const sctx = validateCall(ctx, path[0], false);
  validatePath(path);
  if (path.length !== 1) {
    throw new Error('findMember can only be called at the table level');
  }
  if (gIsServer) {
    const accountID = await DataReader.findMember(sctx, path[0], searchFields);
    return await DataReader.getDataForFields(
      sctx,
      accountID,
      path,
      {
        memberDataOnly: true,
        allowServerData: true,
        fieldMask: fieldMask,
        recursiveFetch: !fieldMask,
      },
    );
  }
  if (gIsClient) {
    throw new Error('findMember can only run on the server');
  }
  throw new Error('Sketch is not initialized');
}

export async function getMemberData(ctx: ServerContext, path: string[], accountID: AccountID, fieldMask: OptMask) {
  const sctx = validateCall(ctx, path[0], false);
  validatePath(path);
  if (!accountID) {
    throw new Error('getMemberData passed an invalid accountID');
  }
  if (gIsServer) {
    return await DataReader.getDataForFields(
      sctx,
      accountID,
      path,
      {
        memberDataOnly: true,
        allowServerData: true,
        recursiveFetch: !fieldMask,
        fieldMask: fieldMask,
      },
    );
  } else if (gIsClient) {
    throw new Error('getMemberData can only run on the server');
  } else {
    throw new Error('Sketch is not initialized');
  }
}

export async function initializeMember(ctx: OptContext, path: string[], accountID: AccountID) {
  if (gIsClient) {
    return await modifyMember('insert', ctx, path, accountID, DataHelpers.clientDataForMembership(path, accountID));
  }
  if (gIsServer) {
    const found = await isMember(ctx as ServerContext, path, accountID);
    if (!found) {
      await modifyMember('insert', ctx, path, accountID, DataHelpers.clientDataForMembership(path, accountID));
    }
    return;
  }
  throw new Error('Sketch is not initialized');
}

// insert/update/upsert
async function modifyMember(modifyMethod: ModifyMethod, ctx: OptContext, path: string[], accountID: AccountID, memberData: any) {
  const sctx = validateCall(ctx, path[0], true);
  validatePath(path);
  if (gIsServer || gIsClient) {
    return await DataWriter.modifyData(sctx, modifyMethod, accountID, path, memberData, { memberDataOnly: true, allowServerData: true });
  }
  throw new Error('Sketch is not initialized');
}

export async function insertMember(ctx: OptContext, path: string[], accountID: AccountID, memberData: any) {
  await modifyMember('insert', ctx, path, accountID, memberData);
}

export async function updateMember(ctx: OptContext, path: string[], accountID: AccountID, memberData: any) {
  await modifyMember('update', ctx, path, accountID, memberData);
}

export async function incrementMemberField(ctx: ServerContext, path: string[], accountID: AccountID, incrementAmount: number) {
  const sctx = validateCall(ctx, path[0], true);
  validatePath(path);
  if (gIsServer) {
    return await modifyMember('increment', sctx, path, accountID, incrementAmount);
  }
  if (gIsClient) {
    throw new Error('incrementMemberField can only run on the server');
  }
  throw new Error('Sketch is not initialized');
}

async function removeMemberEx(ctx: OptContext, path: string[], accountID: AccountID, errorOnNotFound: boolean) {
  const sctx = validateCall(ctx, path[0], true);
  validatePath(path);
  if (gIsServer || gIsClient) {
    const fieldFilter = {
      allowServerData: true,
      memberDataOnly: true,
      recursiveFetch: true,
    };
    let p = DataWriter.removeData(sctx, accountID, path, fieldFilter);
    if (!errorOnNotFound) {
      p = ignoreError(p, 'not found');
    }
    return await p;
  }
  throw new Error('Sketch is not initialized');
}

export async function removeMember(ctx: OptContext, path: string[], accountID: AccountID) {
  await removeMemberEx(ctx, path, accountID, true);
}

export async function removeMemberIfExists(ctx: OptContext, path: string[], accountID: AccountID) {
  await removeMemberEx(ctx, path, accountID, false);
}

export function isValidTable(tableName: string) : boolean {
  const table = TableDef.getTable(tableName, false);
  return !!table;
}

export function isSyncedTable(tableName: string) : boolean {
  const table = TableDef.getTable(tableName, false);
  return table ? table.isSynced : false;
}

export function isSubscriptionTable(tableName: string) : boolean {
  const table = TableDef.getTable(tableName, false);
  return table ? table.type === 'subscription' : false;
}

export function getTableStorageType(tableName: string) : string {
  const table = TableDef.getTable(tableName, false);
  return (table && table.storeOnWindow) ? 'window' : 'default';
}

export function getAllClientTables(ctx: ServerContext, dbOut: Stash, tableFilter: string[] | null) {
  const sctx = validateCall(ctx, null, false);

  // allow sql fetches to run in parallel
  const oldParallelFetch = sctx.sketchCanParallelFetch;
  sctx.sketchCanParallelFetch = true;

  const jobs = new Jobs.Parallel(100, { results: dbOut });

  const tableNames = TableDef.getTableList();
  for (let i = 0; i < tableNames.length; ++i) {
    const tableName = tableNames[i];
    if (tableFilter && tableFilter.indexOf(tableName) < 0) {
      continue;
    }
    const table = TableDef.getTable(tableName, false);
    if (!table || !table.isSynced || table.type === 'subscription') {
      continue;
    }
    jobs.collate(tableName, getClientTable, sctx, tableName);
  }

  // handle table subscriptions
  for (let key in sctx.user.tableSubs) {
    if (tableFilter && tableFilter.indexOf(key) < 0) {
      continue;
    }
    const path = key.split(':');
    const tableName = path[0];
    const table = TableDef.getTable(tableName, false);
    if (!table || !table.isSynced || table.type !== 'subscription') {
      continue;
    }
    jobs.add(getClientDataForSubscription, sctx, path, key, dbOut);
  }

  jobs.drain(function(err, results) {
    sctx.sketchCanParallelFetch = oldParallelFetch;
    cb(err, results);
  });
}

export function addTablesToSchema(dbSchema: Types.Schema) {
  const tableNames = TableDef.getTableList();
  for (let i = 0; i < tableNames.length; ++i) {
    const tableName = tableNames[i];
    const table = TableDef.getTable(tableName, true);
    dbSchema[tableName] = table.getSchema();
  }
}

export async function getAllAccountIDs(ctx: ServerContext) {
  const sctx = validateCall(ctx, 'account', false);
  if (!gIsServer) {
    throw new Error('getAllAccountIDs can only run on the server');
  }
  const sketchTable = TableDef.getTable('account');
  if (!sketchTable) {
    throw new Error('missing account table');
  }
  return await DataReader.lookupMemberAccountIDs(sctx, sketchTable, {});
}

export async function getMemberAccountIDs(ctx: ServerContext, path: string[]) {
  const sctx = validateCall(ctx, path[0], false);
  validatePath(path);
  if (!gIsServer) {
    throw new Error('getMemberAccountIDs can only run on the server');
  }
  if (path.length !== 2) {
    throw new Error('getMemberAccountIDs can only be called at the root object level');
  }
  const sketchTable = TableDef.getTable(path, true);
  if (sketchTable.type !== 'shared' && sketchTable.type !== 'personal') {
    throw new Error('getMemberAccountIDs can only be run on a shared or personal table type');
  }
  const knownKeys = {};
  const objPath = path.slice(1);
  DataHelpers.walkPathForIds(sketchTable.dataSchema, objPath, knownKeys);

  // shared lock for read
  await DataReader.lockMembership(sctx, sketchTable, knownKeys, false);
  return await DataReader.lookupMemberAccountIDs(sctx, sketchTable, knownKeys);
}


export {
  MAP,
 } from './objSchema';

export {
  EACH_MEMBER,
  ACCOUNT_MAP,
  PERSONA_MAP,
  defineSharedTable,
  definePersonalTable,
  defineSubscriptionTable,
  defineGlobalTable,
  addIndex,
} from './TableDef';

export {
  defineAction,
  defineActionProgram,
  registerServerAction,
  registerAsyncServerAction,
  runAction,
  runActionNoSync,
  runActionWithArgs,
  replayAction,
  dispatchAction,
  clientUUID,
  clientTime,
  serverTime,
  getActionSchema,
} from './Actions';

export {
  clientDataForMembership,
  serverDataForCreate,
} from './DataHelpers';
