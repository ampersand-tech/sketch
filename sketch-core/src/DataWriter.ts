/**
 * Copyright 2015-present Ampersand Technologies, Inc.
 */

import * as DataHelpers from './DataHelpers';
import * as DataReader from './DataReader';
import { SketchContext, SketchServerContext, SqlInterface, SqlTable } from './SketchTypes';
import * as TableDef from './TableDef';
import { SketchTable } from './TableDef';
import * as Types from './types';

import * as ObjUtils from 'amper-utils/dist/objUtils';
import { wrap } from 'amper-utils/dist/promiseUtils';
import { Stash, StashOf } from 'amper-utils/dist/types';


const FEED_ACTIONS = {
  insert: 'create',
  upsert: 'upsert',
  update: 'update',
  remove: 'remove',
};

const gShowQueries = false;
const gShowErrors = true;

let gIsClient = false;
let DataStore;
let Feed;
let Sql: SqlInterface;

type ModifyMethod = 'insert'|'update'|'upsert'|'increment'|'remove';

interface DataToWrite {
  sqlRows?: Stash;
  privateFeedUpdates: Stash;
  sharedFeedUpdates: Stash;
  fullFeedUpdateAccountID?: AccountID;
  sqlIncrementValues?: Stash;
}

export function init(feedModule, dataStoreModule, sqlModule: SqlInterface|undefined) {
  Feed = feedModule;
  DataStore = dataStoreModule;
  Sql = sqlModule!;

  gIsClient = !!DataStore;
}

async function incrementField(ctx: SketchContext, sqlTable: SqlTable, rowIds, fields, dataToWrite: DataToWrite) {
  const value = await Sql.tableIncrementField(ctx.sql, sqlTable, rowIds, fields);
  ObjUtils.copyFields(value, dataToWrite.sqlIncrementValues);
}

async function sqlUpdate(
  ctx: SketchContext,
  sketchTable: SketchTable,
  sqlTable: SqlTable,
  rowIds: Stash,
  fields: Stash,
) {
  const count = await Sql.tableUpdate(ctx.sql, sqlTable, rowIds, fields);
  if (count) {
    return;
  }

  const isOptionalRow = sqlTable.isParallelStore || !sketchTable.isRootMap || (
    sqlTable.primaryKeys.length > 2 && sqlTable.primaryKeys[0] === 'accountID'
  );
  if (!isOptionalRow) {
    console.error('Sketch: sql set failed for required row', { sqlTable: sqlTable.name, rowIds: rowIds, fields: fields });
    return;
  }

  // need to do an insert with default values for unspecified fields
  console.debug('Sketch: sql set failed; no rows updated', { sqlTable: sqlTable.name, rowIds: rowIds, fields: fields });
  for (const fieldPath in sketchTable.fields) {
    const field = sketchTable.fields[fieldPath];
    if (field.sqlTableName !== sqlTable.name) {
      continue;
    }
    if (fields.hasOwnProperty(field.sqlFieldName!)) {
      continue;
    }
    fields[field.sqlFieldName!] = field.type._toSql(field.type._getDefaultValue());
  }

  // rowIds need to be in the fields object
  ObjUtils.copyFields(rowIds, fields);

  await Sql.tableInsert(ctx.sql, sqlTable, fields);
}

async function writeSqlRows(ctx: SketchContext, sketchTable: SketchTable, modifyMethod: ModifyMethod, dataToWrite: DataToWrite) {
  // write in order from fewest ids to most ids
  const rowKeys = Object.keys(dataToWrite.sqlRows!);
  rowKeys.sort(function(k0, k1) {
    return k0.split('/').length - k1.split('/').length;
  });

  if (modifyMethod === 'remove') {
    // remove in reverse order
    rowKeys.reverse();
  }

  for (const rowKey of rowKeys) {
    const fields = dataToWrite.sqlRows![rowKey];
    const key = rowKey.split('/');
    const sqlTable = sketchTable.sqlTables[key[0]];
    if (!sqlTable) {
      gShowErrors && console.log('SketchError.badRowKey', {
        table: sketchTable.name,
        sqlTable: key[0],
        rowKey,
        details: 'writeSqlRows could not find sql table for row key',
      });
      throw new Error('Found bad rowKey: ' + rowKey);
    }

    const rowIds = {};
    for (let j = 1; j < key.length;  ++j) {
      const keyName = sqlTable.primaryKeys[j - 1];
      if (!key[j]) {
        if (modifyMethod !== 'remove') {
          gShowErrors && console.log('SketchError.badKeys', {
            table: sketchTable.name,
            key: keyName,
            rowKey,
            details: 'writeSqlRows found missing value for key',
          });
          throw new Error('Bad keys for non-remove!');
        }
        continue;
      }
      if (modifyMethod === 'insert') {
        fields[keyName] = key[j];
      } else {
        rowIds[keyName] = key[j];
      }
    }

    if (modifyMethod === 'insert') {
      gShowQueries && console.log('Sketch: ' + sqlTable.name + '.insert:', {fields: fields});
      await Sql.tableInsert(ctx.sql, sqlTable, fields);
    } else if (modifyMethod === 'upsert') {
      gShowQueries && console.log('Sketch: ' + sqlTable.name + '.upsert:', {query: rowIds, fields: fields});
      await Sql.tableUpsert(ctx.sql, sqlTable, rowIds, fields);
    } else if (modifyMethod === 'remove') {
      if (Object.keys(rowIds).length < 1) {
        gShowErrors && console.log('SketchError.noKeys', {
          table: sketchTable.name,
          rowKey,
          details: 'writeSqlRows tried to execute a remove with no keys, which would delete the entire table',
        });
        throw new Error('Tried to do a sql remove with no keys!');
      }
      gShowQueries && console.log('Sketch: ' + sqlTable.name + '.remove:', {query: rowIds});
      await Sql.tableRemove(ctx.sql, sqlTable, rowIds);
    } else if (modifyMethod === 'increment') {
      gShowQueries && console.log('Sketch: ' + sqlTable.name + '.increment:', {query: rowIds, fields: fields});
      await incrementField(ctx, sqlTable, rowIds, fields, dataToWrite);
    } else {
      gShowQueries && console.log('Sketch: ' + sqlTable.name + '.set:', {query: rowIds, fields: fields});
      await sqlUpdate(ctx, sketchTable, sqlTable, rowIds, fields);
    }
  }
}

async function feedAddMulti(ctx: SketchServerContext, accountIDs: AccountID[], feedPath: string, fields: Stash) {
  const keys = feedPath.split('/');

  if (Feed) {
    const sketchTable = TableDef.getTable(keys);
    if (!sketchTable || !sketchTable.isSynced) {
      return;
    }
    if (!ctx.sketchFeedToWrite) {
      await wrap(Feed.addMulti, ctx, accountIDs, keys, fields, ctx.clientKey);
      return;
    }

    ctx.sketchFeedToWrite.push({
      accountIDs: accountIDs,
      keys: keys,
      fields: fields,
      clientKey: ctx.clientKey,
    });
    return;
  }

  if (accountIDs.indexOf(ctx.user.id) < 0) {
    return;
  }

  const action = fields._action;
  delete fields._action;

  let clientKey: string;
  if (ctx.sketchActionData && ctx.sketchActionData.clientKey) {
    clientKey = ctx.sketchActionData.clientKey;
  } else {
    clientKey = Util.randInt().toString();
  }

  DataStore.changeDataAsInternal(action, keys, fields, clientKey, false, ctx.sketchNoWatchTrigger);
}

function populateDefaultFeedValues(_ctx, fieldFilter, knownKeys, feedPath, feedData) {
  if (feedData._action !== 'create') {
    return;
  }

  // populate default values for insert
  const keys = feedPath.split('/');
  const objPath = keys.slice(1);
  const sketchTable = TableDef.getTable(keys, true);

  if (fieldFilter.allowMemberData || fieldFilter.memberDataOnly) {
    DataHelpers.populateDefaultValues(sketchTable.memberDataSchema, feedData, knownKeys, objPath, true);
  }
  if (!fieldFilter.memberDataOnly) {
    DataHelpers.populateDefaultValues(sketchTable.sharedDataSchema, feedData, knownKeys, objPath, true);
  }
}

async function writeToFeed(ctx: SketchServerContext, fieldFilter, knownKeys: Stash, dataToWrite: DataToWrite, memberAccountIDs: AccountID[]) {
  if (ctx.noFeedWrite) {
    return;
  }

  const sharedFeedUpdates = dataToWrite.sharedFeedUpdates;
  const privateFeedUpdates = dataToWrite.privateFeedUpdates;
  let feedPath;
  let feedData;

  const sharedFeedAccounts = {};
  for (feedPath in sharedFeedUpdates) {
    sharedFeedAccounts[feedPath] = memberAccountIDs.slice(0);
    if (dataToWrite.fullFeedUpdateAccountID) {
      Util.removeByVal(sharedFeedAccounts[feedPath], dataToWrite.fullFeedUpdateAccountID);
    }
  }

  // order matters on these!

  // send private feed updates
  for (const _accountID in privateFeedUpdates) {
    const accountID = _accountID as AccountID;
    const accountFeedUpdates = privateFeedUpdates[accountID];
    for (feedPath in accountFeedUpdates) {
      feedData = accountFeedUpdates[feedPath];
      const sharedFeedData = sharedFeedUpdates[feedPath];
      if (sharedFeedData) {
        // merge in shared data and remove account from the list for the shared feed update
        ObjUtils.copyFieldsIfUnset(sharedFeedData, feedData);
        Util.removeByVal(sharedFeedAccounts[feedPath], accountID);
      }
      populateDefaultFeedValues(ctx, fieldFilter, knownKeys, feedPath, feedData);
      await feedAddMulti(ctx, [accountID], feedPath, feedData);
    }
  }

  // send shared feed updates
  for (feedPath in sharedFeedUpdates) {
    feedData = sharedFeedUpdates[feedPath];
    const feedAccountIDs = sharedFeedAccounts[feedPath];
    if (feedAccountIDs.length) {
      populateDefaultFeedValues(ctx, fieldFilter, knownKeys, feedPath, feedData);
      await feedAddMulti(ctx, feedAccountIDs, feedPath, feedData);
    }
  }
}

function validateField(fieldDesc, value, objPath, fieldFilter) {
  if (!Types.validateType(value, fieldDesc.type)) {
    return 'invalid type for value of field ' + objPath.join('/');
  }
  if (fieldDesc.memberData && !fieldFilter.allowMemberData && !fieldFilter.memberDataOnly) {
    return 'memberData field ' + objPath.join('/') + ' is not allowed for this call';
  }
  if (!fieldDesc.memberData && fieldFilter.memberDataOnly) {
    return 'field ' + objPath.join('/') + ' is not memberData, but only member data is allowed for this call';
  }
  return null;
}

function addFeedToTarget(feedTarget: Stash, objPath: string[], sketchTable: SketchTable, modifyMethod: ModifyMethod) {
  const isRemove = modifyMethod === 'remove';
  const feedObjPath = objPath.slice(0, isRemove ? undefined : -1);
  const feedPath = [sketchTable.name].concat(feedObjPath).join('/');
  if (!feedTarget[feedPath]) {
    feedTarget[feedPath] = {
      _action: FEED_ACTIONS[modifyMethod],
    };
  }
  return feedTarget[feedPath];
}

function addFieldToWrite(
  dataToWrite: DataToWrite,
  fieldDesc: Stash,
  objPath: string[],
  knownKeys: Stash,
  data: any,
  sketchTable: SketchTable,
  fieldFilter,
  modifyMethod: ModifyMethod,
) {
  // validate
  const err = validateField(fieldDesc, data, objPath, fieldFilter);
  if (err) { return err; }

  if (dataToWrite.sqlRows && fieldDesc.name !== 'id') {
    // add to sql writes
    const sqlTable = sketchTable.sqlTables[fieldDesc.sqlTableName];
    const rowKey = TableDef.buildRowKey(sqlTable, knownKeys);
    if (!dataToWrite.sqlRows[rowKey]) {
      dataToWrite.sqlRows[rowKey] = {};
    }
    dataToWrite.sqlRows[rowKey][fieldDesc.sqlFieldName] = fieldDesc.type._toSql(data);
  }

  // add to feed writes
  let feedTarget: StashOf<Stash>|null = null;
  if (fieldDesc.serverOnly || fieldDesc.noFeed || (!sketchTable.isClientVisible)) {
    // no feed update
  } else if (modifyMethod === 'increment') {
    return 'increment not allowed for client visible fields';
  } else if (fieldDesc.memberData && !fieldDesc.eachMember) {
    // feed update for single member only
    if (!knownKeys.accountID) {
      throw new Error('found member fieldDesc without an accountID for field "' + fieldDesc.name + '"');
    }
    feedTarget = dataToWrite.privateFeedUpdates as StashOf<Stash>;
    if (!feedTarget[knownKeys.accountID]) {
      feedTarget[knownKeys.accountID] = {};
    }
    feedTarget = feedTarget[knownKeys.accountID];
  } else {
    // shared feed update
    feedTarget = dataToWrite.sharedFeedUpdates;
  }

  if (feedTarget) {
    const feedData = addFeedToTarget(feedTarget, objPath, sketchTable, modifyMethod);
    feedData[fieldDesc.name] = data;
  }

  return null;
}

// TODO this needs unit tests
function gatherDataToWrite(
  ctx: SketchContext,
  dataToWrite: DataToWrite,
  schema,
  objPath: string[],
  knownKeys: Stash,
  data: any,
  sketchTable: SketchTable,
  fieldFilter,
  modifyMethod: ModifyMethod,
) {
  if (DataHelpers.isFieldDesc(schema)) {
    // leaf node
    return addFieldToWrite(dataToWrite, schema, objPath, knownKeys, data, sketchTable, fieldFilter, modifyMethod);
  }

  const schemaKeys = Object.keys(schema);
  const isMap = schemaKeys.length === 1 && schemaKeys[0][0] === '$';
  const idName = isMap ? schemaKeys[0].slice(1) : null;
  let key, schemaValue, err;
  const nonLeaves: any[] = [];

  if (isMap && modifyMethod === 'upsert') {
    gShowErrors && console.log('SketchError.invalidCall', {
      table: sketchTable.name,
      path: objPath,
      details: 'gatherDataToWrite cannot upsert with nested maps',
    });
    return 'cannot upsert with nested maps';
  }

  // iterate over fields
  for (key in data) {
    const subPath = objPath.concat([key]);
    if (!DataHelpers.isValidKey(key)) {
      gShowErrors && console.log('SketchError.invalidKey', {
        table: sketchTable.name,
        path: subPath,
        details: 'gatherDataToWrite invalid key',
      });
      return 'invalid key "' + key + '" found in data at "' + subPath.join('/') + '"';
    }
    schemaValue = isMap ? schema[schemaKeys[0]] : schema[key];
    if (!schemaValue) {
      gShowErrors && console.log('SketchError.unknownField', {
        table: sketchTable.name,
        path: subPath,
        details: 'gatherDataToWrite unknown field',
      });
      return 'unknown field "' + subPath.join('/') + '" in data';
    }

    let subKeys = knownKeys;
    if (isMap && idName) {
      subKeys = Util.clone(subKeys);
      subKeys[idName] = key;
    }

    if (DataHelpers.isFieldDesc(schemaValue)) {
      // leaf node
      err = addFieldToWrite(dataToWrite, schemaValue, subPath, subKeys, data[key], sketchTable, fieldFilter, modifyMethod);
      if (err) {
        return err;
      }
    } else if (data[key] === null || data[key] === undefined) {
      delete data[key];
    } else {
      nonLeaves.push({
        schemaValue: schemaValue,
        subPath: subPath,
        subKeys: subKeys,
        data: data[key],
      });
    }
  }

  if (!isMap && modifyMethod === 'insert') {
    // make sure every non-nullable schema field is present in data
    for (key in schema) {
      if (key === 'id') {
        continue;
      }
      schemaValue = schema[key];
      if (data.hasOwnProperty(key)) {
        continue;
      }
      if (DataHelpers.isFieldDesc(schemaValue)) {
        if (fieldFilter.memberDataOnly && !schemaValue.memberData) {
          if (sketchTable.type !== 'shared') {
            continue;
          }
          if (objPath.length === 1) {
            // root insert
            continue;
          }
          gShowErrors && console.log('SketchError.invalidCall', {
            table: sketchTable.name,
            path: objPath,
            details: 'gatherDataToWrite invalid insertMember call on shared table',
          });
          return 'invalid insertMember call on shared table ' + objPath.join('/');
        }
        if (schemaValue.memberData && !fieldFilter.allowMemberData && !fieldFilter.memberDataOnly) {
          continue;
        }
        if (schemaValue.type._nullable) {
          err = addFieldToWrite(dataToWrite, schemaValue, objPath.concat([key]), knownKeys, null, sketchTable, fieldFilter, modifyMethod);
          if (err) {
            return err;
          }
          continue;
        }
      } else if (!DataHelpers.schemaPassesFilter(schemaValue, fieldFilter)) {
        continue;
      }
      gShowErrors && console.log('SketchError.missingValue', {
        table: sketchTable.name,
        path: objPath.concat([key]),
        details: 'gatherDataToWrite missing value for required field',
      });
      return 'missing value for required field ' + objPath.join('/') + '/' + key;
    }
  }

  // recurse
  for (const nonLeaf of nonLeaves) {
    err = gatherDataToWrite(
      ctx,
      dataToWrite,
      nonLeaf.schemaValue,
      nonLeaf.subPath,
      nonLeaf.subKeys,
      nonLeaf.data,
      sketchTable,
      fieldFilter,
      modifyMethod,
    );
    if (err) {
      return err;
    }
  }

  return null;
}

async function getFullFeedData(ctx: SketchContext, fullObjPath: string[], accountID: AccountID, dataToWrite: DataToWrite) {
  if (!accountID) {
    throw new Error('no accountID in getFullFeedData');
  }
  const data = await DataReader.getDataForFields(ctx, accountID, fullObjPath, { allowMemberData: true, recursiveFetch: true });

  // replace feed updates with a single insert
  data._action = FEED_ACTIONS.insert;
  dataToWrite.privateFeedUpdates[accountID] = {};
  dataToWrite.privateFeedUpdates[accountID][fullObjPath.join('/')] = data;

  dataToWrite.fullFeedUpdateAccountID = accountID;
}

async function getMemberFeedData(ctx: SketchContext, fullObjPath: string[], dataToWrite: DataToWrite, accountID: AccountID) {
  const data = await DataReader.getDataForFields(ctx, accountID, fullObjPath, { allowMemberData: true, recursiveFetch: true });

  data._action = FEED_ACTIONS.insert;
  dataToWrite.privateFeedUpdates[accountID] = {};
  dataToWrite.privateFeedUpdates[accountID][fullObjPath.join('/')] = data;
}

async function getFullFeedDataForRestore(ctx: SketchContext, fullObjPath: string[], dataToWrite: DataToWrite, memberAccountIDs: AccountID[]) {
  for (const accountID of memberAccountIDs) {
    await getMemberFeedData(ctx, fullObjPath, dataToWrite, accountID);
  }
}

async function mergeFeedDataForInsert(fullObjPath: string[], accountID: AccountID|undefined, dataToWrite: DataToWrite) {
  const rootPath = fullObjPath.join('/');

  const data = {
    _action: FEED_ACTIONS.insert,
  };

  let err;

  function mergeFeed(mergeFeedPath, feedData) {
    if (mergeFeedPath.slice(0, rootPath.length) !== rootPath) {
      err = 'bad feedPath found: ' + mergeFeedPath + '  ' + rootPath;
      return;
    }

    let subPath = mergeFeedPath.slice(rootPath.length);
    if (subPath.length && subPath[0] !== '/') {
      err = 'bad feedPath found: ' + mergeFeedPath + '  ' + rootPath;
      return;
    }

    subPath = subPath.length ? subPath.slice(1).split('/') : [];
    for (const field in feedData) {
      if (field !== '_action') {
        Util.objectFillPath(data, subPath.concat([field]), feedData[field]);
      }
    }
  }

  const feedUpdates = dataToWrite.sharedFeedUpdates;
  for (const feedPath in feedUpdates) {
    mergeFeed(feedPath, feedUpdates[feedPath]);
  }

  if (err) {
    throw new Error(err);
  }

  // replace feed updates with a single insert
  if (accountID) {
    const privateFeedUpdates = dataToWrite.privateFeedUpdates[accountID] || {};
    for (const feedPath in privateFeedUpdates) {
      mergeFeed(feedPath, privateFeedUpdates[feedPath]);
    }

    if (err) {
      throw new Error(err);
    }

    dataToWrite.privateFeedUpdates[accountID] = {};
    dataToWrite.privateFeedUpdates[accountID][rootPath] = data;
    dataToWrite.fullFeedUpdateAccountID = accountID;
  } else {
    dataToWrite.sharedFeedUpdates = {};
    dataToWrite.sharedFeedUpdates[rootPath] = data;
  }
}

async function conditionallyLookupMemberIDs(
  ctx: SketchServerContext,
  sketchTable: SketchTable,
  knownKeys: Stash,
  dataToWrite: DataToWrite,
) {
  if (Object.keys(dataToWrite.sharedFeedUpdates).length <= 0) {
    return [];
  }

  // shared lock for reading
  await DataReader.lockMembership(ctx, sketchTable, knownKeys, false);
  return await DataReader.lookupMemberAccountIDs(ctx, sketchTable, knownKeys);
}

/*
  algorithm for data modify:
  - recurse data
  -- error on restricted fields
  -- error on type mismatch
  -- generate sql rows to write
  -- generate feed diffs (for self and other members)
  - write to sql
  - if there are any feed diffs for each member, do membership query
  - write to feed

  fieldFilter: {
    allowServerData: boolean
    memberDataOnly: boolean
    allowMemberData: boolean
  }
 */
export async function modifyData(
  ctx: SketchServerContext,
  modifyMethod: ModifyMethod,
  accountID: AccountID|undefined,
  objPath: string[],
  data: any,
  fieldFilter,
) {
  console.debug('Sketch.modifyData', { userID: ctx.user && ctx.user.id, modifyMethod, accountID, objPath, data, fieldFilter });

  const sketchTable = TableDef.getTable(objPath, true);

  if (sketchTable.type === 'global' || sketchTable.type === 'subscription') {
    if (fieldFilter.memberDataOnly) {
      throw new Error('table "' + sketchTable.name + '" does not support membership functions');
    }
    accountID = undefined;
  } else if (sketchTable.type === 'personal') {
    if (!(fieldFilter.memberDataOnly || fieldFilter.allowMemberData) || !accountID) {
      throw new Error('table "' + sketchTable.name + '" does not support modifyServerData');
    }
    fieldFilter.memberDataOnly = true;
  }

  const fullObjPath = objPath;
  objPath = objPath.slice(1);

  if (sketchTable.useRootDefaults && objPath.length === 0 && modifyMethod === 'insert') {
    throw new Error('table "' + sketchTable.name + '" does not support insert at the root');
  }

  const knownKeys: Stash = accountID ? { accountID: accountID } : {};
  const rootIdPath = accountID ? [ 'accountID', accountID ] : [];
  const rootFieldPathArr = [ sketchTable.name ];
  const schema = DataHelpers.walkPathForIds(sketchTable.dataSchema, objPath, knownKeys, rootIdPath, rootFieldPathArr);
  const rootFieldPath = rootFieldPathArr.join('/') + '/';

  const dataToWrite: DataToWrite = {
    sqlRows: gIsClient ? undefined : {},
    privateFeedUpdates: {},
    sharedFeedUpdates: {},
    fullFeedUpdateAccountID: undefined,
    sqlIncrementValues: gIsClient ? undefined : {},
  };

  // recurse data to generate feed updates and sql writes
  const gatherErr = gatherDataToWrite(ctx, dataToWrite, schema, objPath, knownKeys, data, sketchTable, fieldFilter, modifyMethod);
  if (gatherErr) {
    throw new Error(gatherErr);
  }

  const isMembershipChange = modifyMethod === 'insert' && fieldFilter.memberDataOnly && sketchTable.type === 'shared' && fullObjPath.length === 2;

  await DataReader.verifyModifyAccess(
    ctx,
    accountID,
    sketchTable,
    rootIdPath,
    rootFieldPath,
    fullObjPath,
    modifyMethod,
    fieldFilter.memberDataOnly,
  );

  if (isMembershipChange) {
    // exclusive lock for writing
    await DataReader.lockMembership(ctx, sketchTable, knownKeys, true);
  }

  if (!gIsClient) {
    // write to sql
    await writeSqlRows(ctx, sketchTable, modifyMethod, dataToWrite);
  }

  // fixup feed
  if (modifyMethod === 'insert') {
    // fetch the full object if adding a member to a shared root object
    if (isMembershipChange && !gIsClient) {
      // special case for insertMember: need to fetch the full data object and add it to feed
      if (!accountID) {
        throw new Error('missing accountID');
      }
      await getFullFeedData(ctx, fullObjPath, accountID, dataToWrite);
    } else {
      // make sure it isn't a server-only insert
      const idFieldDesc = sketchTable.fields[rootFieldPath + 'id'];
      if (!idFieldDesc || !idFieldDesc.serverOnly) {
        // merge down feed updates for accountID into a single full insert and fill in empty maps
        const mergeForAll = !fieldFilter.memberDataOnly && sketchTable.type === 'shared' && fullObjPath.length > 2;
        await mergeFeedDataForInsert(fullObjPath, mergeForAll ? undefined : accountID, dataToWrite);
      }
    }
  }

  // look up members if sending shared feed updates
  let memberAccountIDs: AccountID[] = [];
  if (sketchTable.type === 'personal') {
    if (!accountID) {
      throw new Error('missing accountID');
    }
    memberAccountIDs = [accountID];
  } else if (gIsClient) {
    memberAccountIDs = [ctx.user.id];
  } else if (!sketchTable.isSynced) {
    memberAccountIDs = [];
  } else if (sketchTable.type === 'subscription') {
    memberAccountIDs = [ fullObjPath.slice(0, 2).join(':') as AccountID ];
  } else if (sketchTable.type === 'global') {
    memberAccountIDs = [ '*' as AccountID ];
  } else {
    memberAccountIDs = await conditionallyLookupMemberIDs(ctx, sketchTable, knownKeys, dataToWrite);
  }

  // write to feed
  await writeToFeed(ctx, fieldFilter, knownKeys, dataToWrite, memberAccountIDs);

  if (modifyMethod === 'increment') {
    return dataToWrite.sqlIncrementValues![fullObjPath[fullObjPath.length - 1]];
  }
}

function generateSqlRemoves(sketchTable, filteredFields, knownKeys, sqlRows) {
  for (const fieldPath in filteredFields) {
    const sqlTableName = filteredFields[fieldPath].sqlTableName;
    const sqlTable = sketchTable.sqlTables[sqlTableName];
    const rowKey = TableDef.buildRowKey(sqlTable, knownKeys);
    if (!sqlRows[rowKey]) {
      sqlRows[rowKey] = {};
    }
  }
}

function generateFeedForRemove(
  _ctx: ServerContext,
  accountID: AccountID|undefined,
  sketchTable: SketchTable,
  schema,
  objPath: string[],
  filteredFields,
  knownKeys: Stash,
  fieldFilter,
  dataToWrite: DataToWrite,
) {
  let feedData;

  if (fieldFilter.memberDataOnly) {
    if (!accountID) {
      throw new Error('missing accountID');
    }

    // special case for removeMember: just do a full remove at the root for the removed member
    dataToWrite.privateFeedUpdates[accountID] = {};
    feedData = addFeedToTarget(dataToWrite.privateFeedUpdates[accountID], objPath, sketchTable, 'remove');
    dataToWrite.fullFeedUpdateAccountID = accountID;

    // do eachMember removes for everyone else
    for (const fieldPath in filteredFields) {
      let fieldPathArray = fieldPath.split('/');
      const accountIdx = fieldPathArray.indexOf('$accountID');
      if (accountIdx < 0) {
        // not an eachMember path
        continue;
      }

      // remove at eachMember map root
      fieldPathArray = fieldPathArray.slice(1, accountIdx + 1);

      // convert to actual objPath by filling in known IDs
      for (let i = 0;  i < fieldPathArray.length;  ++i) {
        let key = fieldPathArray[i];
        if (key[0] !== '$') {
          continue;
        }
        key = key.slice(1);
        if (!knownKeys.hasOwnProperty(key)) {
          gShowErrors && console.log('SketchError.missingKey', {
            table: sketchTable.name,
            path: objPath,
            knownKeys,
            fieldPath,
            details: 'generateFeedForRemove knownKeys missing key for field',
          });
          throw new Error('missing key for "' + key + '"');
        }
        fieldPathArray[i] = knownKeys[key];
      }

      addFeedToTarget(dataToWrite.sharedFeedUpdates, fieldPathArray, sketchTable, 'remove');
    }
  } else {
    // do a single shared feed remove at the root for everyone
    feedData = addFeedToTarget(dataToWrite.sharedFeedUpdates, objPath, sketchTable, 'remove');
  }

  if (DataHelpers.schemaIsMap(schema)) {
    feedData._replaceWithObject = 1;
  }
}

// TODO there is still a problem of missing intermediate IDs in this code.
// for example if an eachMember map is contained within another map and removeMember is called it needs to lookup the intermediate ids
// this needs to be handled for the sql removes as well
export async function removeData(
  ctx: SketchServerContext,
  accountID: AccountID|undefined,
  objPath: string[],
  fieldFilter,
) {
  console.debug('Sketch.removeData', { userID: ctx.user && ctx.user.id, accountID, objPath, fieldFilter });

  const sketchTable = TableDef.getTable(objPath, true);

  if (sketchTable.type === 'global' || sketchTable.type === 'subscription') {
    if (fieldFilter.memberDataOnly) {
      throw new Error('table "' + sketchTable.name + '" does not support membership functions');
    }
    accountID = undefined;
  } else if (sketchTable.type === 'personal') {
    if (!(fieldFilter.memberDataOnly || fieldFilter.allowMemberData) || !accountID) {
      throw new Error('table "' + sketchTable.name + '" does not support removeServerData');
    }
    fieldFilter.memberDataOnly = true;
  }

  const fullObjPath = objPath;
  objPath = objPath.slice(1);
  const isRootRemove = sketchTable.isRootMap ? objPath.length === 1 : objPath.length === 0;

  if (sketchTable.useRootDefaults && isRootRemove) {
    throw new Error('table "' + sketchTable.name + '" does not support remove at the root');
  }

  const dataToWrite: DataToWrite = {
    sqlRows: gIsClient ? undefined : {},
    privateFeedUpdates: {},
    sharedFeedUpdates: {},
    fullFeedUpdateAccountID: undefined,
  };

  const rootIdPath = accountID ? [ 'accountID', accountID ] : [];
  const knownKeys: Stash = {};
  const removeForAllMembers = isRootRemove && !fieldFilter.memberDataOnly;
  if (accountID && !removeForAllMembers) {
    knownKeys.accountID = accountID;
  }
  const rootFieldPathArr = [ sketchTable.name ];
  const schema = DataHelpers.walkPathForIds(sketchTable.dataSchema, objPath, knownKeys, rootIdPath, rootFieldPathArr);
  const rootFieldPath = rootFieldPathArr.join('/') + '/';

  const filteredFields = TableDef.applyFieldFilter(sketchTable, rootFieldPath, fieldFilter);
  if (typeof filteredFields === 'string') {
    // filteredFields is an error string
    gShowErrors && console.log('SketchError.fieldFilterError', {
      table: sketchTable.name,
      path: fullObjPath,
      err: filteredFields,
      details: 'removeData error from applyFieldFilter',
    });
    throw new Error(filteredFields);
  }

  const isMembershipChange = fieldFilter.memberDataOnly && sketchTable.type === 'shared' && fullObjPath.length === 2;

  let privateMemberDataOnly = !isRootRemove;
  for (const fieldPath in filteredFields) {
    const fieldData = filteredFields[fieldPath];
    const sqlTableName = fieldData.sqlTableName;
    if (sqlTableName === sketchTable.primarySqlTableName) {
      isPrimaryRemove = true;
    }
    if (!fieldData.memberData || fieldData.eachMember) {
      privateMemberDataOnly = false;
    }
  }


  await DataReader.verifyModifyAccess(
    ctx,
    accountID,
    sketchTable,
    rootIdPath,
    rootFieldPath,
    fullObjPath,
    'remove',
    fieldFilter.memberDataOnly,
  );

  if (isMembershipChange) {
    // exclusive lock for writing
    await DataReader.lockMembership(ctx, sketchTable, knownKeys, true);
  }

  // membership query
  let memberAccountIDs: AccountID[] = [];
  if (sketchTable.type === 'personal' || privateMemberDataOnly) {
    if (!accountID) {
      throw new Error('missing accountID');
    }
    memberAccountIDs = [accountID];
  } else if (gIsClient) {
    memberAccountIDs = [ctx.user.id];
  } else if (!sketchTable.isSynced) {
    memberAccountIDs = [];
  } else if (sketchTable.type === 'subscription') {
    memberAccountIDs = [ fullObjPath.slice(0, 2).join(':') as AccountID ];
  } else if (sketchTable.type === 'global') {
    memberAccountIDs = [ '*' as AccountID ];
  } else {
    // shared lock for reading
    await DataReader.lockMembership(ctx, sketchTable, knownKeys, false);

    memberAccountIDs = await DataReader.lookupMemberAccountIDs(ctx, sketchTable, knownKeys);
  }

  if (!gIsClient) {
    // generate sql remove queries by iterating over field descs
    await generateSqlRemoves(sketchTable, filteredFields, knownKeys, dataToWrite.sqlRows);

    // write to sql after looking up members so we know to whom to send feed updates
    await writeSqlRows(ctx, sketchTable, 'remove', dataToWrite);
  }

  // make sure it isn't a server-only remove
  const idFieldDesc = sketchTable.fields[rootFieldPath + 'id'];
  if (!idFieldDesc || !idFieldDesc.serverOnly) {
    // generate feed updates
    await generateFeedForRemove(ctx, accountID, sketchTable, schema, objPath, filteredFields, knownKeys, fieldFilter, dataToWrite);

    // write to feed
    await writeToFeed(ctx, fieldFilter, knownKeys, dataToWrite, memberAccountIDs);
  }
}
