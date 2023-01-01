/**
 * Copyright 2015-present Ampersand Technologies, Inc.
 */

import * as SketchData from './DataHelpers';
import * as Sketch from './index';
import { SketchContext, SketchServerContext, SqlInterface, SqlTable } from './SketchTypes';
import * as SketchTable from './TableDef';

import * as ErrorUtils from 'amper-utils/dist/errorUtils';
import { ParallelQueue, wrap } from 'amper-utils/dist/promiseUtils';
import { Stash, StashOf } from 'amper-utils/dist/types';


let gIsClient = false;
let DataStore;
let Sql: SqlInterface;

const ID_BATCH_SIZE = 50;

const gShowQueries = false;
const gShowErrors = true;

interface FetchedData {
  ids: Stash;
  sqlRows: StashOf<Stash>;
  invalidPaths: string[][];
}


export function init(dataStoreModule, sqlModule: SqlInterface|undefined) {
  DataStore = dataStoreModule;
  Sql = sqlModule!;
  gIsClient = !!DataStore;
}

async function tableFind(ctx: SketchContext, sqlTable: SqlTable, query: Stash|undefined, opts: Stash|undefined, fieldMask: Stash|undefined) {
  let sqlLink: OptSqlLink = undefined;
  if (ctx.sql && ctx.sketchCanParallelFetch && !Sql.isTransactLink(ctx.sql)) {
    sqlLink = Sql.allocShared(ctx, true);
    if (sqlLink) {
      sqlLink.overrideQueryTimeout = ctx.sql.overrideQueryTimeout;
    }
  }

  try {
    return await Sql.tableFind(sqlLink || ctx.sql, sqlTable, query, opts, fieldMask);
  } catch (err) {
    if (gShowErrors) {
      console.log('SketchError.sqlError', { table: sqlTable.name, query, err });
    }
    throw err;
  } finally {
    if (sqlLink) {
      Sql.free(sqlLink);
      sqlLink = undefined;
    }
  }
}

function constructQuery(fetchPath: string[], rootIdPath?: string[], idBatch?: [string, string][]) {
  let idLength = 0;
  if (rootIdPath) {
    idLength += rootIdPath.length;
  }

  let batchName;
  if (idBatch) {
    idLength += 2;
    batchName = idBatch[0][0];
  }

  if (!idLength) {
    return null;
  }

  const query = {};
  for (let i = 0;  i < fetchPath.length;  ++i) {
    let idName = fetchPath[i];
    if (idName === '*') {
      continue;
    }
    const idIndex = i * 2;
    if (rootIdPath && idIndex < rootIdPath.length) {
      idName = rootIdPath[idIndex];
      query[idName] = rootIdPath[idIndex + 1];
    } else if (idBatch && idIndex < idLength) {
      for (let j = 0;  j < idBatch.length;  ++j) {
        idName = idBatch[j][0];
        const idValue = idBatch[j][1];
        if (idName !== batchName) {
          throw new Error('bad batching');
        }
        if (!query.hasOwnProperty(idName)) {
          query[idName] = { $in: [] };
        }
        query[idName]['$in'].push(idValue);
      }
    } else if (query.hasOwnProperty(idName)) {
      // handles the case of [ 'accountID', 'draftsID', 'accountID' ] where the second accountID is not in the rootIdPath
      delete query[idName];
    }
  }

  return query;
}

async function fetchSqlData(
  ctx: SketchContext,
  params: {
    sqlOpts?: any;
    sketchTable: SketchTable.SketchTable;
    sqlTable: any;
    fieldMask: any;
    query?: any;
    fetch?: {
      path: string[];
      data: FetchedData;
      rootIdPath: string[];
      idBatch?: [string, string][];
    },
    isRequired?: boolean;
  },
) {
  let query = params.query;
  if (!query && params.fetch) {
    query = constructQuery(params.fetch.path, params.fetch.rootIdPath, params.fetch.idBatch);
  }

  const isPrimary = params.sqlTable.name === params.sketchTable.primarySqlTableName;

  const results = await tableFind(ctx, params.sqlTable, query, params.sqlOpts, params.fieldMask);

  if (params.isRequired && (!results || !results.length)) {
    gShowErrors && console.log('SketchError.notFound', {
      table: params.sketchTable.name,
      sqlTable: params.sqlTable.name,
      query,
      details: 'fetchSqlData could not find any rows matching the required query' },
    );
    throw new Error('not found');
  }

  gShowQueries && console.log('Sketch: ' + params.sqlTable.name + '.find:', {query, results});

  if (params.fetch) {
    const rows = results || [];

    let batchName;
    let batchesMissing;

    if (isPrimary && params.sketchTable.isRootMap) {
      if (params.fetch.idBatch) {
        batchName = params.fetch.idBatch[0][0];
        batchesMissing = {};
        for (const ids of params.fetch.idBatch) {
          batchesMissing[ids[1]] = 1;
        }
      } else if (!rows.length) {
        params.fetch.data.invalidPaths.push(params.fetch.rootIdPath);
      }
    }

    for (const row of rows) {
      if (batchesMissing) {
        const rowBatchValue = row[batchName];
        delete batchesMissing[rowBatchValue];
      }

      // store fetched fields
      const rowKey = SketchTable.buildRowKey(params.sqlTable, row);
      /* for debugging
      if (params.fetchedData.sqlRows[rowKey] && Util.safeStringify(row) !== Util.safeStringify(params.fetchedData.sqlRows[rowKey])) {
        console.warn('Sketch.rowKeyAlreadyFetched', {table: sqlTable.name, query, rowKey, row, oldRow: params.fetchedData.sqlRows[rowKey]});
      }
      */
      params.fetch.data.sqlRows[rowKey] = row;

      // update fetch.data.ids
      let idPath: string[] = [];
      for (let j = 0; j < params.fetch.path.length;  ++j) {
        let idName = params.fetch.path[j];
        let idValue;
        if (j * 2 < params.fetch.rootIdPath.length) {
          idName = params.fetch.rootIdPath[j * 2];
          idValue = params.fetch.rootIdPath[j * 2 + 1];
        } else if (idName === '*') {
          continue;
        } else {
          idValue = row[idName];
        }
        idPath = idPath.concat([idName, idValue]);
      }

      let ids = params.fetch.data.ids;
      for (const name of idPath) {
        if (ids[name] === undefined) {
          ids[name] = {};
        }
        ids = ids[name];
      }
    }

    for (const batchValue in batchesMissing) {
      params.fetch.data.invalidPaths.push(params.fetch.rootIdPath.concat([batchName, batchValue]));
    }
  }

  if (results && !results.length) {
    return undefined;
  }
  return results;
}

function populateFieldDataFromRow(sketchTable: SketchTable.SketchTable, field, knownKeys: StashOf<string>, sqlRows: StashOf<Stash>, data: Stash) {
  const sqlTable = sketchTable.sqlTables[field.sqlTableName];
  const rowKey = SketchTable.buildRowKey(sqlTable, knownKeys);
  const row = sqlRows[rowKey];
  if (row) {
    data[field.name] = field.type._fromSql(row[field.sqlFieldName]);
  } else {
    // use default
    data[field.name] = field.type._getDefaultValue();
  }
}

function populateClientData(
  sketchTable: SketchTable.SketchTable,
  fieldPath: string[],
  field,
  data: Stash,
  knownKeys: StashOf<string>,
  idTable: Stash|undefined,
  sqlRows: StashOf<Stash>,
) {
  const p = fieldPath[0];
  fieldPath = fieldPath.slice(1);

  if (!fieldPath.length) {
    // get field value from sql data
    if (!field.noFeed) {
      populateFieldDataFromRow(sketchTable, field, knownKeys, sqlRows, data);
    }
    return;
  }

  // recurse
  if (p[0] === '$') {
    if (!idTable) {
      return;
    }
    knownKeys = Util.clone(knownKeys);
    const idName = p.slice(1);
    const ids = idTable[idName] || {};
    for (const idValue in ids) {
      knownKeys[idName] = idValue;
      data[idValue] = data[idValue] || {};
      populateClientData(sketchTable, fieldPath, field, data[idValue], knownKeys, ids[idValue], sqlRows);
    }
  } else {
    data[p] = data[p] || {};
    populateClientData(sketchTable, fieldPath, field, data[p], knownKeys, idTable, sqlRows);
  }
}

function extractRootIDs(ids: Stash, rootIdPath: string[]) {
  const idObj = Util.objectGetFromPath(ids, rootIdPath);
  const idList: [string, string][] = [];

  for (const idName in idObj) {
    const idValues = idObj[idName];
    for (const idValue in idValues) {
      idList.push([idName, idValue]);
    }
  }

  return idList;
}

function removePath(ids: Stash|undefined, idPath: string[]) {
  for (let i = 0; i < idPath.length - 1;  ++i) {
    ids = ids && ids[idPath[i]];
  }
  if (ids) {
    delete ids[idPath[idPath.length - 1]];
  }
}

async function doFetchPass(
  ctx: SketchContext,
  sqlOpts,
  sketchTable: SketchTable.SketchTable,
  rootIdPath,
  fetchPaths,
  fetchedData: FetchedData,
  pathStart: number,
  pathEnd: number,
  idList?: [string, string][],
) {
  const jobs = new ParallelQueue();

  for (let i = pathStart; i < pathEnd; ++i) {
    const fetchPath = fetchPaths[i];
    for (const sqlTableDesc of fetchPath.sqlTables) {
      const sqlTable = sketchTable.sqlTables[sqlTableDesc.sqlTableName];
      if (!idList) {
        jobs.add(fetchSqlData, ctx, {
          sqlOpts,
          sketchTable,
          sqlTable,
          fieldMask: sqlTableDesc.sqlFieldMask,
          fetch: {
            rootIdPath,
            path: fetchPath.path,
            data: fetchedData,
          },
          isRequired: sqlTableDesc.isRequired,
        });
        continue;
      }

      for (let k = 0; k < idList.length; k += ID_BATCH_SIZE) {
        const batch = idList.slice(k, k + ID_BATCH_SIZE);
        jobs.add(fetchSqlData, ctx, {
          sqlOpts,
          sketchTable,
          sqlTable,
          fieldMask: sqlTableDesc.sqlFieldMask,
          fetch: {
            rootIdPath,
            idBatch: batch,
            path: fetchPath.path,
            data: fetchedData,
          },
          isRequired: sqlTableDesc.isRequired,
        });
      }
    }
  }

  await jobs.run();
}

async function serverFetchData(
  ctx: SketchContext,
  accountID: AccountID|undefined,
  objPath: string[],
  sqlOpts,
  sketchTable: SketchTable.SketchTable,
  filteredFields,
  rootIdPath,
  fetchPaths,
) {
  // prefill fetchedData.ids with rootIdPath
  const fetchedData: FetchedData = {
    ids: {},
    sqlRows: {},
    invalidPaths: [],
  };
  Util.objectFillPath(fetchedData.ids, rootIdPath, {});


  // fetch the data from sql

  const needTwoPass = rootIdPath.length === 2 && rootIdPath[0] === 'accountID' && sketchTable.type === 'shared';

  if (needTwoPass) {
    // need to do a pass to fetch the IDs the user has access to, then include those in the path for the second pass
    await doFetchPass(ctx, sqlOpts, sketchTable, rootIdPath, fetchPaths, fetchedData, 0, 1);
    const rootIds = extractRootIDs(fetchedData.ids, rootIdPath);
    await doFetchPass(ctx, sqlOpts, sketchTable, rootIdPath, fetchPaths, fetchedData, 1, fetchPaths.length, rootIds);
  } else {
    await doFetchPass(ctx, sqlOpts, sketchTable, rootIdPath, fetchPaths, fetchedData, 0, fetchPaths.length);
  }

  // remove invalid ID paths
  for (let i = 0; i < fetchedData.invalidPaths.length; ++i) {
    const idPath = fetchedData.invalidPaths[i];
    removePath(fetchedData.ids, idPath);
  }

  const data = {};
  const knownKeys: Stash = accountID ? { accountID } : {};
  const rootIdTable = accountID ? fetchedData.ids.accountID[accountID] : fetchedData.ids;
  for (const fieldPath in filteredFields) {
    const path = fieldPath.split('/').slice(1);
    const field = sketchTable.fields[fieldPath];
    populateClientData(sketchTable, path, field, data, knownKeys, rootIdTable, fetchedData.sqlRows);
  }

  return Util.objectGetFromPath(data, objPath);
}

async function makeFetchRequired(
  ctx: SketchContext,
  sketchTable: SketchTable.SketchTable,
  sqlTable: SqlTable,
  rootIdPath: string[],
  fetchPaths: SketchTable.FetchPath[] | undefined,
  accountID: AccountID|undefined,
) {
  let alreadyFetching = false;

  // don't trigger separate fetch if already fetching from the necessary table
  if (fetchPaths) {
    for (let i = 0; i < fetchPaths.length; ++i) {
      let validFetchPath = true;

      if (accountID) {
        // make sure the fetch path is for the correct accountID fetch
        const path = fetchPaths[i].path;
        if (path.lastIndexOf('accountID') > 0) {
          // this fetch path is within an EACH_MEMBER block, so it won't be fetching by a specific accountID
          validFetchPath = false;
        }
      }

      if (!validFetchPath) {
        continue;
      }

      const tables = fetchPaths[i].sqlTables;
      for (let j = 0; j < tables.length; ++j) {
        if (tables[j].sqlTableName === sqlTable.name) {
          alreadyFetching = true;
          tables[j].isRequired = true;
        }
      }
    }
  }

  if (alreadyFetching) {
    return;
  }

  // need a separate fetch
  const query: Stash = {};
  const mask: StashOf<1> = {};
  for (let i = 0; i < rootIdPath.length; i += 2) {
    const idName = rootIdPath[i];
    if (sqlTable.primaryKeys.indexOf(idName) < 0) {
      continue;
    }
    const idValue = rootIdPath[i + 1];
    query[idName] = idValue;
    mask[idName] = 1;
  }

  if (accountID) {
    query.accountID = accountID;
  }

  return await fetchSqlData(ctx, {
    sketchTable,
    sqlTable,
    fieldMask: mask,
    query,
    isRequired: true,
  });
}

async function verifyAccount(ctx: SketchContext, sketchTable: SketchTable.SketchTable, accountID: AccountID) {
  if (gIsClient) {
    return;
  }

  try {
    await wrap(Sketch.getMemberData, ctx as any, ['account'], accountID, { verify: 1 });
  } catch (err) {
    if (ErrorUtils.isNotFound(err)) {
      gShowErrors && console.log('SketchError.notAllowed', {
        table: sketchTable.name,
        accountID,
        details: 'verifyAccount could not find a user with that accountID',
      });
      throw new Error('not allowed');
    }
    throw err;
  }
}

async function verifyMembership(
  ctx: SketchContext,
  accountID: AccountID,
  sketchTable: SketchTable.SketchTable,
  rootIdPath: string[],
  rootFieldPath: string,
  fullObjPath: string[],
  modifyMethod: 'get'|'remove',
  fetchPaths,
) {
  if (gIsClient) {
    if (accountID !== ctx.user.id) {
      if (modifyMethod !== 'remove') {
        gShowErrors && console.log('SketchError.badAccountID', {
          table: sketchTable.name,
          details: 'verifyMembership called on the client with an accountID other than the current user',
        });
        throw new Error('bad accountID');
      }

      const fieldPath = SketchTable.getAccountVerifyFieldPath(sketchTable, rootFieldPath);
      if (!fieldPath) {
        gShowErrors && console.log('SketchError.cannotVerifyMembership', {
          table: sketchTable.name,
          details: 'verifyMembership called on the client with an accountID other than the current user and the table has no EACH_MEMBER map',
        });
        throw new Error('cannot verify membership, no EACH_MEMBER map');
      }

      fullObjPath = fieldPath.split('/');
      if (rootIdPath[0] === 'accountID') {
        rootIdPath = rootIdPath.slice(2);
      }

      let idPos = 0;
      for (let i = 0;  i < fullObjPath.length;  ++i) {
        const p = fullObjPath[i];
        if (p[0] !== '$') {
          continue;
        }
        const wantIdName = p.slice(1);
        if (wantIdName === 'accountID') {
          fullObjPath[i] = accountID;
        } else {
          const idName = rootIdPath[idPos];
          const idValue = rootIdPath[idPos + 1];
          idPos += 2;
          if (idName !== wantIdName) {
            gShowErrors && console.log('SketchError.cannotVerifyMembership', {
              table: sketchTable.name,
              idName,
              path: fullObjPath,
              rootIdPath,
              details: 'verifyMembership could not extract a necessary ID from the rootIdPath',
            });
            throw new Error('cannot verify membership, id mismatch');
          }
          fullObjPath[i] = idValue;
        }
      }
    }
    if (!DataStore.hasData(fullObjPath.slice(0, 2))) {
      gShowErrors && console.log('SketchError.notFound', {
        table: sketchTable.name,
        path: fullObjPath.slice(0, 2),
        details: 'verifyMembership found no data at path',
      });
      throw new Error('not found');
    }
    return;
  }

  if (sketchTable.useRootDefaults && rootIdPath.length === 2 && rootIdPath[0] === 'accountID') {
    return;
  }

  const membershipSqlTable = sketchTable.sqlTables[sketchTable.membershipSqlTableName!];
  await makeFetchRequired(ctx, sketchTable, membershipSqlTable, rootIdPath, fetchPaths, accountID);
}

function findExistenceTable(sketchTable: SketchTable.SketchTable, rootIdPath: string[], includeAccountID) {
  const existencePath: string[] = [];
  let start = 0;

  // for personal tables the unique key includes the accountID
  if (!includeAccountID && rootIdPath[0] === 'accountID') {
    start = 2;
  }

  for (let i = start; i < rootIdPath.length; i += 2) {
    existencePath.push(rootIdPath[i]);
  }

  for (const sqlTableName in sketchTable.sqlTables) {
    const sqlTable = sketchTable.sqlTables[sqlTableName];
    if (!sqlTable.isParallelStore && SketchTable.tableCanFetchData(sqlTable, existencePath)) {
      return sqlTable;
    }
  }
  return null;
}

async function verifyExistence(
  ctx: SketchContext,
  sketchTable: SketchTable.SketchTable,
  rootIdPath: string[],
  fullObjPath: string[],
  fetchPaths: SketchTable.FetchPath[] | undefined,
) {
  if (gIsClient) {
    if (!DataStore.hasData(fullObjPath)) {
      gShowErrors && console.log('SketchError.notFound', {
        table: sketchTable.name,
        path: fullObjPath,
        details: 'verifyExistence found no data at path',
      });
      throw new Error('not found');
    }
    return;
  }

  if (sketchTable.useRootDefaults && rootIdPath.length === 2 && rootIdPath[0] === 'accountID') {
    return;
  }

  const existenceSqlTable = findExistenceTable(sketchTable, rootIdPath, false) || findExistenceTable(sketchTable, rootIdPath, true);
  if (!existenceSqlTable) {
    gShowErrors && console.log('SketchError.noTableFound', {
      table: sketchTable.name,
      rootIdPath,
      details: 'verifyExistence could not find an existence table',
    });
    throw new Error('no table found');
  }

  return await makeFetchRequired(ctx, sketchTable, existenceSqlTable, rootIdPath, fetchPaths, undefined);
}

async function verifyNonExistence(ctx: SketchContext, sketchTable: SketchTable.SketchTable, fullObjPath: string[]) {
  if (!gIsClient) {
    return;
  }

  if (DataStore.hasData(fullObjPath)) {
    gShowErrors && console.log('SketchError.alreadyExists', {
      table: sketchTable.name,
      path: fullObjPath,
      details: 'verifyNonExistence found data already at path',
    });
    throw new Error('already exists');
  }
}

async function verifyTableAccess(ctx: SketchContext, accountID: AccountID|undefined, sketchTable: SketchTable.SketchTable) {
  if (!accountID) {
    // ServerData function, allow access
    return;
  }
  if (!ctx.user || ctx.user.id !== accountID) {
    // MemberData function, allow access
    return;
  }

  // run permissions checks
  for (const permFunc of sketchTable.perms) {
    await wrap(permFunc, ctx);
  }
}

async function verifyGetAccess(
  ctx: SketchContext,
  accountID: AccountID|undefined,
  sketchTable: SketchTable.SketchTable,
  rootIdPath: string[],
  rootFieldPath: string,
  fullObjPath: string[],
  fetchPaths,
) {
  try {
    await verifyTableAccess(ctx, accountID, sketchTable);
  } catch (_err) {
    throw new Error('not found');
  }

  let objDepth = rootIdPath.length / 2;
  if (sketchTable.isRootMap && accountID) {
    objDepth--;
  }

  if (!objDepth) {
    // access at the root, no checks to do
    return;
  }

  if (accountID && sketchTable.type !== 'personal') {
    // getClientData or getMemberData
    await verifyMembership(ctx, accountID, sketchTable, rootIdPath, rootFieldPath, fullObjPath, 'get', fetchPaths);
  }

  await verifyExistence(ctx, sketchTable, rootIdPath, fullObjPath, fetchPaths);
}

export async function verifyModifyAccess(
  ctx: SketchContext,
  accountID: AccountID|undefined,
  sketchTable,
  rootIdPath,
  rootFieldPath,
  fullObjPath,
  modifyMethod,
  memberDataOnly,
) {
  try {
    await verifyTableAccess(ctx, accountID, sketchTable);
  } catch (err) {
    throw new Error('not allowed');
  }

  let objDepth = rootIdPath.length / 2;
  if (sketchTable.isRootMap && accountID) {
    objDepth--;
  }

  if (!objDepth) {
    // access at the root, no checks to do
    gShowErrors && console.log('SketchError.notAllowed', {
      table: sketchTable.name,
      path: fullObjPath,
      details: 'verifyModifyAccess passed a path to the table root',
    });
    throw new Error('not allowed');
  }

  const isInsert = modifyMethod === 'insert' || modifyMethod === 'upsert';
  const isRootInsert = isInsert && objDepth === 1;

  const userID = (ctx.user && ctx.user.id) || '';
  if (isInsert && accountID && accountID !== userID && sketchTable.name !== 'account') {
    await verifyAccount(ctx, sketchTable, accountID);
  }

  if (modifyMethod === 'insert' && !memberDataOnly && gIsClient) {
    await verifyNonExistence(ctx, sketchTable, fullObjPath);
  }

  if (memberDataOnly && isRootInsert) {
    // insertMember, verify object exists
    if (sketchTable.type !== 'personal') {
      await verifyExistence(ctx, sketchTable, rootIdPath, fullObjPath, undefined);
    }
  } else if (memberDataOnly && !isInsert) {
    // updateMember or removeMember
    const noMembership = !gIsClient && sketchTable.type === 'personal' && sketchTable.name !== 'account';
    if (!noMembership) {
      await verifyMembership(ctx, accountID!, sketchTable, rootIdPath, rootFieldPath, fullObjPath, modifyMethod, null);
    }

    if (noMembership || objDepth > 1) {
      // need additional existence verify on the client, otherwise feed operations will error
      await verifyExistence(ctx, sketchTable, rootIdPath, fullObjPath, undefined);
    }
  } else if (!isRootInsert) {
    if (accountID && sketchTable.type !== 'personal') {
      // modifyClientData or removeClientData
      await verifyMembership(ctx, accountID, sketchTable, rootIdPath, rootFieldPath, fullObjPath, modifyMethod, null);
    }

    if (isInsert) {
      // verify parent object exists for insert
      await verifyExistence(ctx, sketchTable, rootIdPath.slice(0, -2), fullObjPath.slice(0, -1), undefined);
    } else {
      // verify object exists for modify/remove
      await verifyExistence(ctx, sketchTable, rootIdPath, fullObjPath, undefined);
    }
  }
}

function populateFieldDataFromObj(field, dataOut, dataIn) {
  if (Object.prototype.hasOwnProperty.call(dataIn, field.name)) {
    dataOut[field.name] = dataIn[field.name];
  } else {
    // use default
    dataOut[field.name] = field.type._getDefaultValue();
  }
}

function populateClientDataFromObj(fieldPath, field, dataOut, dataIn) {
  const p = fieldPath[0];
  fieldPath = fieldPath.slice(1);

  if (!fieldPath.length) {
    if (!field.noFeed) {
      // get field value from sql data
      populateFieldDataFromObj(field, dataOut, dataIn);
    }
    return;
  }

  // recurse
  if (p[0] === '$') {
    for (const idValue in dataIn) {
      dataOut[idValue] = dataOut[idValue] || {};
      populateClientDataFromObj(fieldPath, field, dataOut[idValue], dataIn[idValue]);
    }
  } else {
    dataOut[p] = dataOut[p] || {};
    populateClientDataFromObj(fieldPath, field, dataOut[p], dataIn[p] || {});
  }
}

function clientFetchData(ctx: SketchContext, sketchTable: SketchTable.SketchTable, fullObjPath: string[], filteredFields) {
  const data = DataStore.getDataUnsafe(fullObjPath);
  if (data === undefined) {
    console.error('Sketch.clientFetchData reached with non-existent path; this should have been caught earlier');
    gShowErrors && console.log('SketchError.notFound', {
      table: sketchTable.name,
      path: fullObjPath,
      details: 'clientFetchData could not find data at the path',
    });
    throw new Error('not found');
  }

  if (data === null && !Object.keys(filteredFields).length) {
    gShowErrors && console.log('SketchError.notFound', {
      table: sketchTable.name,
      path: fullObjPath,
      details: 'clientFetchData found null at the path, and no fields were requested (existence check)',
    });
    throw new Error('not found');
  }

  let filteredData: any = {};

  for (const fieldPath in filteredFields) {
    const path = fieldPath.split('/').slice(fullObjPath.length);
    if (!path.length) {
      // fetching single field
      filteredData = data;
    } else if (data === null) {
      gShowErrors && console.log('SketchError.notFound', {
        table: sketchTable.name,
        path: fullObjPath.concat(path),
        details: 'clientFetchData could not find data for field',
      });
      throw new Error('not found');
    } else {
      const field = sketchTable.fields[fieldPath];
      populateClientDataFromObj(path, field, filteredData, data);
    }
  }

  return filteredData;
}

/*
  algorithm for data fetch:
  - extract rootIdPath from objPath and generate a path string with $ids in place of the actual ids
  - prefill fetchedData.ids with rootIdPath
  - filter sketchTable.fields down to the set we need for the query (serverOnly, is subpath of objPath, etc)
  - generate fetchPaths from filtered fields
  - fetch from sql using fetchPaths
  - iterate over filtered fields and call populateClientData to convert from sql rows to object format

  fieldFilter: {
    allowServerData: boolean
    memberDataOnly: boolean
    allowMemberData: boolean
    recursiveFetch: boolean
    fieldMask: boolean
  }
 */
export async function getDataForFields(ctx: SketchContext, accountID: AccountID|undefined, objPath: string[], fieldFilter) {
  console.debug('Sketch.getDataForFields', { userID: ctx.user && ctx.user.id, accountID: accountID, objPath: objPath, fieldFilter: fieldFilter });
  const sketchTable = SketchTable.getTable(objPath, true);

  if (sketchTable.type === 'global' || sketchTable.type === 'subscription') {
    if (fieldFilter.memberDataOnly) {
      throw new Error('table "' + sketchTable.name + '" does not support membership functions');
    }
    if (sketchTable.type === 'subscription' && accountID) {
      if (objPath.length < 2) {
        throw new Error('cannot fetch subscription table "' + sketchTable.name + '" at the root');
      }
      const subKey = objPath[0] + ':' + objPath[1];
      if (ctx.user.tableSubs && !ctx.user.tableSubs.hasOwnProperty(subKey)) {
        gShowErrors && console.log('SketchError.notFound', {
          table: sketchTable.name,
          path: objPath,
          details: 'getDataForFields user does not have the right tableSubs',
        });
        throw new Error('not found');
      }
    }
    accountID = undefined;
  } else if (sketchTable.type === 'personal') {
    if (!(fieldFilter.memberDataOnly || fieldFilter.allowMemberData) || !accountID) {
      console.log('table does not support this call', { accountID, objPath, fieldFilter });
      throw new Error('table "' + sketchTable.name + '" does not support getServerData');
    }
    fieldFilter.memberDataOnly = true;
  }

  if (fieldFilter.fieldMask && Object.keys(fieldFilter.fieldMask).length === 0) {
    // empty field mask means fetch ids
    if (!sketchTable.isRootMap && objPath.length === 1) {
      // no ids available, just check for existence
    } else {
      fieldFilter.fieldMask = { id: 1 };
    }
  }

  const fullObjPath = objPath;
  objPath = objPath.slice(1);

  // extract rootIdPath from objPath and generate a path string with $ids in place of the actual ids
  const rootIdPath = accountID ? [ 'accountID', accountID ] : [];
  const rootFieldPathArr = [ sketchTable.name ];
  SketchData.walkPathForIds(sketchTable.dataSchema, objPath, undefined, rootIdPath, rootFieldPathArr);
  const rootFieldPath = rootFieldPathArr.join('/') + '/';

  const filteredFields = SketchTable.applyFieldFilter(sketchTable, rootFieldPath, fieldFilter);
  if (typeof filteredFields === 'string') {
    // filteredFields is an error string
    gShowErrors && console.log('SketchError.fieldFilterError', {
      table: sketchTable.name,
      path: fullObjPath,
      err: filteredFields,
      details: 'getDataForFields error from applyFieldFilter',
    });
    throw new Error(filteredFields);
  }

  // generate fetchPaths from filtered fields
  const fetchPaths = gIsClient ? null : SketchTable.generateFetchPaths(sketchTable, filteredFields, rootIdPath);

  await verifyGetAccess(ctx, accountID, sketchTable, rootIdPath, rootFieldPath, fullObjPath, fetchPaths);

  if (gIsClient) {
    return await clientFetchData(ctx, sketchTable, fullObjPath, filteredFields);
  } else {
    return await serverFetchData(
      ctx,
      accountID,
      objPath,
      fieldFilter.sqlOpts,
      sketchTable,
      filteredFields,
      rootIdPath,
      fetchPaths,
    );
  }
}

export async function findMember(ctx: SketchContext, tableName: string, searchFields) {
  const sketchTable = SketchTable.getTable(tableName, true);
  const sqlTable = sketchTable.sqlTables[sketchTable.membershipSqlTableName!];

  for (const key in searchFields) {
    if (!sqlTable.schema.hasOwnProperty(key)) {
      throw new Error('search field "' + key + '" not in membership table');
    }
  }

  const results = await tableFind(ctx, sqlTable, searchFields, undefined, { accountID: 1 });
  if (!results || !results.length) {
    gShowErrors && console.log('SketchError.notFound', {
      table: sketchTable.name,
      searchFields,
      details: 'findMember found no rows matching sql query',
    });
    throw new Error('not found');
  }
  if (results.length > 1) {
    gShowErrors && console.log('SketchError.moreThanOneMatch', {
      table: sketchTable.name,
      searchFields,
      details: 'findMember found more than one row matching sql query',
    });
    throw new Error('more than one match');
  }
  return results[0].accountID;
}

export async function isMember(ctx: SketchContext, objPath: string[], accountID: AccountID) {
  const sketchTable = SketchTable.getTable(objPath, true);
  const sqlTable = sketchTable.sqlTables[sketchTable.membershipSqlTableName!];

  const searchFields = {
    accountID: accountID,
  };
  searchFields[sketchTable.name + 'ID'] = objPath[1];

  const results = await tableFind(ctx, sqlTable, searchFields, undefined, { accountID: 1 });
  if (!results || results.length !== 1) {
    return false;
  }
  return true;
}

export async function lookupMemberAccountIDs(ctx: SketchContext, sketchTable: SketchTable.SketchTable, knownKeys: Stash) {
  if (sketchTable.type === 'global' || sketchTable.type === 'subscription') {
    return [];
  }

  const sqlTable = sketchTable.sqlTables[sketchTable.membershipSqlTableName!];

  let keys: any = null;
  const fieldMask: StashOf<1> = {};

  for (let i = 0;  i < sqlTable.primaryKeys.length;  ++i) {
    const keyName = sqlTable.primaryKeys[i];
    fieldMask[keyName] = 1;
    if (keyName === 'accountID') {
      continue;
    }
    if (!knownKeys.hasOwnProperty(keyName)) {
      gShowErrors && console.log('SketchError.missingKey', {
        table: sketchTable.name,
        key: keyName,
        knownKeys,
        details: 'lookupMemberAccountIDs called with a knownKeys map missing a required key',
      });
      throw new Error('lookupMemberAccountIDs failed: missing key "' + keyName + '"');
    }
    keys = keys || {};
    keys[keyName] = knownKeys[keyName];
  }

  if (!fieldMask.accountID) {
    gShowErrors && console.log('SketchError.missingField', {
      table: sketchTable.name,
      sqlTable: sqlTable.name,
      knownKeys,
      details: 'lookupMemberAccountIDs could not find accountID field in sql membership table',
    });
    throw new Error('lookupMemberAccountIDs failed: accountID not in membership table');
  }

  const results = await tableFind(ctx, sqlTable, keys, undefined, fieldMask);

  const accountIDs: AccountID[] = [];
  for (const row of results) {
    accountIDs.push(row.accountID);
  }
  return accountIDs;
}

export async function lockMembership(ctx: SketchServerContext, sketchTable: SketchTable.SketchTable, knownKeys, forUpdate: boolean) {
  if (gIsClient || !ctx.sketchLocks) {
    return;
  }

  const tableName = 'sketch' + sketchTable.name;
  const sqlTable = sketchTable.sqlTables[tableName];
  if (!sqlTable) {
    return;
  }

  let keys: any = null;
  const fieldMask: StashOf<1> = {};
  let lockPath = tableName;

  for (let i = 0;  i < sqlTable.primaryKeys.length;  ++i) {
    const keyName = sqlTable.primaryKeys[i];
    if (!knownKeys.hasOwnProperty(keyName)) {
      gShowErrors && console.log('SketchError.missingKey', {
        table: sketchTable.name,
        key: keyName,
        knownKeys,
        details: 'lockMembership called with a knownKeys map missing a required key',
      });
      throw new Error('lockMembership failed: missing key "' + keyName + '"');
    }
    fieldMask[keyName] = 1;
    keys = keys || {};
    keys[keyName] = knownKeys[keyName];
    lockPath += '/' + knownKeys[keyName];
  }

  let sqlOpts: Stash|undefined;
  if (forUpdate) {
    if (ctx.sketchLocks[lockPath] === 'forUpdate') {
      // already locked for update in this transaction
      return;
    }
    ctx.sketchLocks[lockPath] = 'forUpdate';
    sqlOpts = { forUpdate: 1 };
  } else {
    if (ctx.sketchLocks[lockPath]) {
      // already locked for update or shared mode in this transaction
      return;
    }
    ctx.sketchLocks[lockPath] = 'lockInShareMode';
    sqlOpts = { lockInShareMode: 1 };
  }

  // issue lock with a select
  console.debug('Sketch.lockMembership', { lockPath: lockPath, forUpdate: forUpdate });
  return await tableFind(ctx, sqlTable, keys, sqlOpts, fieldMask);
}
