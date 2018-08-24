/**
 * Copyright 2015-present Ampersand Technologies, Inc.
 */

import * as ObjSchema from './objSchema';
import * as SketchTypes from './SketchTypes';
import * as Types from './types';

import * as JsonUtils from 'amper-utils/dist2017/jsonUtils';
import * as ObjUtils from 'amper-utils/dist2017/objUtils';
import * as StringUtils from 'amper-utils/dist2017/stringUtils';
import { Stash, StashOf } from 'amper-utils/dist2017/types';
//import * as Perms from 'overlib/shared/perms';


interface InField {
  name: string;
  parallelStore: boolean;
  fieldPath: string[];
  type: Types.Type;
  serverOnly: boolean;
  eachMember: boolean;
  memberData: boolean;
  noFeed: boolean;
  deprecated: boolean;
  sqlPath: string[];
  sqlNamePrefix: string[];
  parentTableName?: string;
}

interface Field {
  name: string;
  sqlTableName: null|string;
  sqlFieldName: null|string;
  type: Types.Type;
  serverOnly: boolean;
  eachMember: boolean;
  memberData: boolean;
  noFeed: boolean;
  deprecated: boolean;
}

export interface FetchPath {
  path: string[];
  sqlTables: {
    sqlTableName: string;
    sqlFieldMask: StashOf<1>;
    isRequired?: true;
  }[];
  forceInclude: boolean;
}

interface FieldFilter {
  allowServerData?: boolean;
  allowMemberData?: boolean;
  memberDataOnly?: boolean;
  recursiveFetch?: boolean;
  fieldMask?: Mask;
}

type Mask = 1 | '*' | {[k: string]: Mask};

const gAllTables: StashOf<SketchTable> = {};
const gAllTableNames: string[] = [];
let gSqlInited = false;


function tableError(tableName, msg) {
  throw new Error('Sketch: ' + msg + ' in table descriptor for "' + tableName + '"');
}

function setObjSchemaValueInternal(obj: Types.Schema, path: string[], value: any, mapToClientSchema: boolean) {
  let p: string;
  let i: number;
  for (i = 0; i < path.length - 1; ++i) {
    p = path[i];
    if (mapToClientSchema && p[0] === '$') {
      p = '_ids';
    }
    if (!obj[p]) {
      obj[p] = {};
    }
    obj = obj[p];
  }
  p = path[i];
  obj[p] = value;
}


function setObjSchemaValue(
  objSchema: Types.Schema,
  serverObjSchema: Types.Schema,
  path: string[],
  value: any,
  props: { serverOnly: boolean },
) {
  if (!props.serverOnly) {
    setObjSchemaValueInternal(objSchema, path, value, true);
  }
  setObjSchemaValueInternal(serverObjSchema, path, value, true);
}

function buildSqlTableName(field: InField, sketchTable: SketchTable) {
  const sqlPath = field.sqlPath.slice(0);
  if (!sketchTable.isRootMap && sketchTable.name !== 'account') {
    sqlPath.splice(1, 0, sketchTable.name);
  }

  let tableName = 'sketch' + sqlPath.join('');
  if (field.parallelStore) {
    tableName += 'parallelstore';
  }
  // sql will lowercase this anyway
  return tableName.toLowerCase();
}

function initSqlTable(sketchTable: SketchTable, name: string, field: InField) {
  const sqlTable: SketchTypes.SqlTable = {
    name: name,
    primaryKeys: [],
    isParallelStore: !!field.parallelStore,
    schema: {},
    indexes: [],
  };

  const idIndex = {} as StashOf<1>;

  for (let i = 0; i < field.sqlPath.length; ++i) {
    const isLast = i === field.sqlPath.length - 1;
    const idName = field.sqlPath[i] + 'ID';

    // add id to schema
    sqlTable.schema[idName] = idName === 'accountID' ? Types.ACCOUNTIDSTR : sketchTable.idType;

    // build index
    idIndex[idName] = 1;
    sqlTable.primaryKeys.push(idName);
    const indexParams = {} as SketchTypes.IndexParams;
    if (isLast) {
      indexParams.primary = true;
    }
    sqlTable.indexes.unshift([ JsonUtils.jsonClone(idIndex), indexParams ]);

    if (i > 0) {
      const individualIndex = {};
      individualIndex[idName] = 1;
      sqlTable.indexes.push([ individualIndex, {} ]);
    }
  }

  if (sqlTable.primaryKeys[0] === 'accountID' && sqlTable.primaryKeys.length > 2) {
    // also need indexes without accountID
    for (let i = 2; i < sqlTable.primaryKeys.length; ++i) {
      const indexData = {};
      for (let j = 0; j < i; ++j) {
        indexData[sqlTable.primaryKeys[j + 1]] = 1;
      }
      sqlTable.indexes.push([ indexData, {} ]);
    }
  }

  return sqlTable;
}

function applyTableOptions(table: SketchTable, options: SketchTypes.TableOptions) {
  if (!options) {
    return;
  }
  if (options.windowOnly) {
    table.isSynced = false;
    table.isServerVisible = false;
    table.storeOnWindow = true;
  } else if (options.clientOnly) {
    table.isSynced = false;
    table.isServerVisible = false;
  } else if (options.serverOnly) {
    table.isSynced = false;
    table.isClientVisible = false;
  }
  if (options.allowUnverifiedMembership) {
    table.allowUnverifiedMembership = true;
  }
  if (options.perms) {
    table.perms = Perms.checkPermFuncs({names: [], types: {} }, 'perms', options.perms, tableError);
  }
  if (options.idType) {
    table.idType = options.idType;
  }
}

function validatePathKey(key: string): boolean {
  const m = key.match(/[A-Za-z0-9]+/);
  return !!m && m[0] === key;
}

function processDescriptor(table: SketchTable, fieldDescs: InField[], descriptor: SketchTypes.SchemaRoot, state, isMap: boolean) {
  let subState: InField;

  if (isMap && !(descriptor as Types.SchemaInternalNode).id) {
    // add implicit id field to every map
    subState = JsonUtils.jsonClone(state);
    subState.fieldPath.push('id');
    subState.type = table.idType;
    subState.noFeed = true;
    fieldDescs.push(subState);
  }

  for (const key in descriptor) {
    const value = descriptor[key];
    subState = JsonUtils.jsonClone(state);

    if (key[0] === '_') {
      // directive
      if (key === '_personal') {
        subState.memberData = true;
      } else if (key === '_server') {
        subState.serverOnly = true;
      } else if (key === '_parallelStore') {
        subState.parallelStore = true;
      } else if (key === '_serverParallelStore') {
        subState.serverOnly = true;
        subState.parallelStore = true;
      } else if (key === '_deprecated') {
        subState.deprecated = true;
      } else if (key === '_tableOptions') {
        continue;
      } else {
        tableError(subState.fieldPath[0], 'unhandled directive "' + key + '"');
      }
      processDescriptor(table, fieldDescs, value, subState, false);
      continue;
    }

    if (!validatePathKey(key)) {
      tableError(subState.fieldPath[0], 'illegal key "' + key + '" found in table');
      continue;
    }

    subState.fieldPath.push(key);

    if (value._validateType) {
      // a leaf value/type
      subState.type = value;
      fieldDescs.push(subState);
      continue;
    }

    if (value._ids) {
      // MAP and EACH_MEMBER

      if (value._eachMember) {
        // EACH_MEMBER
        if (subState.fieldPath.length > 3) {
          tableError(subState.fieldPath[0], 'EACH_MEMBER (' + subState.fieldPath.slice(1).join('/') + ' not allowed in a map');
        }
        subState.eachMember = true;
        subState.memberData = true;
        subState.parentTableName = 'account';

        setObjSchemaValue(table.objSchema, table.serverObjSchema, subState.fieldPath.slice(1).concat(['_eachMember']), true, subState);
      } else {
        // use key as new subtable name
        const subtableName = subState.sqlNamePrefix.concat(key).join('_');
        subState.sqlPath.push(subtableName);
        subState.parentTableName = subtableName;

        // subtables are already parallel storage, so don't propogate down
        subState.parallelStore = false;
        subState.sqlNamePrefix = [];
      }

      if (value._personaKeys) {
        setObjSchemaValue(table.objSchema, table.serverObjSchema, subState.fieldPath.slice(1).concat(['_personaKeys']), true, subState);
      } else if (value._accountKeys) {
        setObjSchemaValue(table.objSchema, table.serverObjSchema, subState.fieldPath.slice(1).concat(['_accountKeys']), true, subState);
      }

      table.mapPaths[subState.fieldPath.join('/')] = '$' + subState.parentTableName + 'ID';
      subState.fieldPath.push('$' + subState.parentTableName + 'ID');
      processDescriptor(table, fieldDescs, value._ids, subState, true);
      continue;
    }

    if (ObjUtils.isObject(value)) {
      subState.sqlNamePrefix.push(key);
      processDescriptor(table, fieldDescs, value, subState, false);
      continue;
    }

    tableError(subState.fieldPath[0], 'unhandled key "' + key + '"');
  }
}

function initFieldMask(sqlFieldMasks: Stash, sqlTable: SketchTypes.SqlTable) {
  if (!sqlFieldMasks[sqlTable.name]) {
    sqlFieldMasks[sqlTable.name] = {};
    for (let i = 0; i < sqlTable.primaryKeys.length; ++i) {
      sqlFieldMasks[sqlTable.name][sqlTable.primaryKeys[i]] = 1;
    }
  }
}

export function tableCanFetchData(sqlTable: SketchTypes.SqlTable, fetchPath: string[]) {
  const primaryKeys = [] as string[];
  if (fetchPath.indexOf('accountID') >= 0) {
    // make sure accountID is first
    primaryKeys.push('accountID');
  }
  for (let i = 0; i < fetchPath.length; ++i) {
    const idName = fetchPath[i];
    if (idName !== '*' && primaryKeys.indexOf(idName) < 0) {
      primaryKeys.push(idName);
    }
  }
  return primaryKeys.join('/') === sqlTable.primaryKeys.join('/');
}

export function generateFetchPaths(table: SketchTable, fields: StashOf<Field>, rootIdPath: (string|number)[]): FetchPath[] {
  let fetchPaths: FetchPath[] = [];
  const fetchPathsCache = { accountID: true };
  const sqlFieldMasks = {};
  const hasAccountID = rootIdPath[0] === 'accountID';

  function addFetchPath(fetchPath) {
    const fetchPathKey = fetchPath.join('/');
    if (!fetchPathsCache[fetchPathKey]) {
      fetchPathsCache[fetchPathKey] = true;
      const pathData = {
        path: fetchPath.slice(0),
        sqlTables: [],
        forceInclude: false,
      };
      fetchPaths.push(pathData);
      return pathData;
    }
  }

  for (const key in fields) {
    const field = fields[key];
    const fieldPath = key.split('/');

    const fetchPath = hasAccountID ? [ 'accountID' ] : [];
    for (let i = 0; i < fieldPath.length; ++i) {
      if (fieldPath[i][0] === '$') {
        fetchPath.push(fieldPath[i].slice(1));
      }
    }

    const stripAccount = hasAccountID && !field.memberData;
    if (stripAccount && fetchPath.length > 2) {
      fetchPath[0] = '*';
    }
    addFetchPath(fetchPath);

    if (fetchPath[0] === 'accountID' && (stripAccount || fetchPath.length === 1)) {
      if (stripAccount) {
        fetchPath[0] = '*';
      }
      fetchPath.push('*');
      addFetchPath(fetchPath);
    }

    initFieldMask(sqlFieldMasks, table.sqlTables[field.sqlTableName!]);
    sqlFieldMasks[field.sqlTableName!][field.sqlFieldName!] = 1;
  }

  // make sure to create sqlFieldMasks for all inbetween tables that don't have fields requested
  const rootIdLength = rootIdPath.length / 2;
  for (let i = fetchPaths.length - 1; i >= 0; --i) {
    const oldPathData = fetchPaths[i];
    const fpath = oldPathData.path;
    for (let j = 0; j < fpath.length; ++j) {
      if (j < rootIdLength) {
        // make sure fetch path and rootIdPath match
        if (fpath[j] !== rootIdPath[j * 2]) {
          break;
        }
      } else {
        // rootIdPath too short, need to fetch inbetween ids
        const newPathData = addFetchPath(fpath.slice(0, j + 1));
        if (newPathData) {
          // newly added
          newPathData.forceInclude = true;
        }
      }
    }
  }

  // pair sql tables to fetch with each fetch path
  for (let i = 0; i < fetchPaths.length; ++i) {
    const pathData = fetchPaths[i];
    for (const sqlTableName in table.sqlTables) {
      const sqlTable = table.sqlTables[sqlTableName];
      if (!tableCanFetchData(sqlTable, pathData.path)) {
        continue;
      }

      let fieldMask = sqlFieldMasks[sqlTableName];
      if (!fieldMask) {
        const needTableIDs = sqlTableName === table.membershipSqlTableName || (pathData.forceInclude && !sqlTable.isParallelStore);
        if (needTableIDs) {
          // this is an inbetween table, need the ids but nothing else
          initFieldMask(sqlFieldMasks, sqlTable);
          fieldMask = sqlFieldMasks[sqlTableName];
        } else {
          // no data in this table is needed
          continue;
        }
      }

      pathData.sqlTables.push({
        sqlTableName: sqlTableName,
        sqlFieldMask: fieldMask,
      });
    }
  }

  fetchPaths = fetchPaths.sort(function(p0, p1) {
    return p0.path.length - p1.path.length;
  });

  if (fetchPaths.length) {
    const firstPath = fetchPaths[0].path;
    let exactMatch = true;
    for (let i = 0; i < firstPath.length; ++i) {
      if (firstPath[i] !== rootIdPath[i * 2]) {
        exactMatch = false;
        break;
      }
    }
    if (exactMatch) {
      fetchPaths[0].path.push('*');
    }
  }

  return fetchPaths;
}

type TableType = 'personal' | 'subscription' | 'global' | 'shared';

export class SketchTable {
  name: string;
  type: TableType;
  descriptor: SketchTypes.SchemaRoot;
  isRootMap: boolean;
  isSynced: boolean;
  isClientVisible: boolean;
  isServerVisible: boolean;
  storeOnWindow: boolean;
  useRootDefaults: boolean;
  allowUnverifiedMembership: boolean;
  idType: Types.Type;
  perms: Perms.PermFunc[];
  objSchema: Types.Schema;
  serverObjSchema: Types.Schema;
  dataSchema: Types.Schema;
  memberDataSchema: Types.Schema;
  sharedDataSchema: Types.Schema;
  sqlTables: StashOf<SketchTypes.SqlTable>;
  membershipSqlTableName: null|string;
  primarySqlTableName: null|string;
  fields: StashOf<Field>;
  mapPaths: StashOf<string>;

  constructor(name, descriptor: SketchTypes.SchemaRoot, tableType) {
    this.name = name;
    this.type = tableType;
    this.descriptor = descriptor;
    this.isRootMap = SketchTypes.isSchemaMapNode(descriptor);
    this.isSynced = true;
    this.isClientVisible = true;
    this.isServerVisible = true;
    this.storeOnWindow = false;
    this.useRootDefaults = !this.isRootMap && name !== 'account';
    this.allowUnverifiedMembership = false;
    this.idType = Types.IDSTR;
    this.perms = [];

    this.objSchema = {};
    this.serverObjSchema = {};

    this.dataSchema = {};
    this.memberDataSchema = {};
    this.sharedDataSchema = {};

    this.sqlTables = {};
    this.membershipSqlTableName = null;
    this.primarySqlTableName = null;

    this.fields = {};
    this.mapPaths = {};

    const fieldDescs: InField[] = [];
    const state = {
      fieldPath: [ name ],
      sqlPath: [] as string[],
      sqlNamePrefix: [],
      parentTableName: name,
      type: null,
      serverOnly: false,
      memberData: false,
      eachMember: false,
      parallelStore: false,
      noFeed: false,
      deprecated: false,
    };

    if (this.type === 'personal') {
      state.memberData = true;
    }

    if (this.type === 'subscription' && !this.isRootMap) {
      tableError(name, 'found non-map for a subscription table type');
    }

    if (this.isRootMap) {
      this.objSchema._ids = {};
      this.mapPaths[name] = '$' + name + 'ID';
      state.sqlPath.push(name);
      state.fieldPath.push('$' + name + 'ID');
      const desc = descriptor as SketchTypes.SchemaRootMap;
      applyTableOptions(this, desc._ids._tableOptions!);
      processDescriptor(this, fieldDescs, desc._ids, state, true);
    } else {
      applyTableOptions(this, descriptor._tableOptions!);
      processDescriptor(this, fieldDescs, descriptor, state, false);
    }

    const sqlPaths = {};
    const me: SketchTable = this;

    function addField(field: InField) {
      const fieldName = field.fieldPath[field.fieldPath.length - 1];
      const outField: Field = {
        name: fieldName,
        sqlTableName: null as null|string,
        sqlFieldName: null,
        type: field.type,
        serverOnly: field.serverOnly,
        eachMember: field.eachMember,
        memberData: field.memberData,
        noFeed: field.noFeed,
        deprecated: field.deprecated,
      };

      if (field.memberData && me.type === 'global') {
        tableError(name, 'found member data for a global table type');
      }
      if (field.memberData && me.type === 'subscription') {
        tableError(name, 'found member data for a subscription table type');
      }

      if (field.eachMember && me.type !== 'shared') {
        tableError(name, 'found EACH_MEMBER for a ' + me.type + ' table type');
      }

      if (field.memberData) {
        field.sqlPath.unshift('account');
      }
      if (!field.sqlPath.length) {
        tableError(name, 'empty sqlPath for field "' + fieldName + '"');
      }

      if (fieldName === 'id') {
        sqlPaths[field.sqlPath.join('/') + '/_dummy_'] = 0;
      } else {
        sqlPaths[field.sqlPath.join('/')] = 1;

        if (fieldName === 'accountID' || StringUtils.endsWith(fieldName, 'AccountID')) {
          if (field.type._sqlTypeName !== 'ACCOUNTIDSTR') {
            tableError(name, fieldName + ' should be of type ACCOUNTIDSTR');
          }
        } else if (field.type._sqlTypeName === 'IDSTR') {
          if (fieldName.slice(-2) !== 'ID') {
            tableError(name, 'ID with invalid name "' + fieldName + '", it should end with ID');
          }
          // TODO foreign key lookup?
        }
      }

      // generate sqlSchema
      const sqlTableName = buildSqlTableName(field, me);
      let sqlTable = me.sqlTables[sqlTableName];
      if (!sqlTable) {
        sqlTable = initSqlTable(me, sqlTableName, field);
        me.sqlTables[sqlTableName] = sqlTable;
      }
      outField.sqlTableName = sqlTableName;

      let sqlFieldName = field.sqlNamePrefix.concat(fieldName).join('_');
      if (outField.name === 'id') {
        // special field name to reference own table's ID
        // somewhat odd logic to handle EACH_MEMBER and _parallelStore
        sqlFieldName = field.parentTableName + 'ID';

        // should already be in sql table
        if (!sqlTable.schema[sqlFieldName]) {
          tableError(name, 'ruh roh');
        }

        if (me.isServerVisible && outField.type._sqlTypeName !== me.idType._sqlTypeName && outField.type._sqlTypeName !== 'ACCOUNTIDSTR') {
          tableError(name, 'invalid type for "id" field, must be ' + me.idType._sqlTypeName + ' or ACCOUNTIDSTR (' + field.fieldPath.join('/') + ')');
        }
      } else {
        // add to sql table
        if (sqlTable.schema[sqlFieldName]) {
          tableError(name, 'duplicate sql field "' + sqlFieldName + '" in sql table "' + sqlTableName + '"');
        }
        sqlTable.schema[sqlFieldName] = field.type;
      }
      outField.sqlFieldName = sqlFieldName;

      // generate objSchema
      const schemaPath = field.fieldPath.slice(1);
      if (!outField.noFeed) {
        setObjSchemaValue(me.objSchema, me.serverObjSchema, schemaPath, field.type, outField);
      }

      // generate dataSchemas
      setObjSchemaValueInternal(me.dataSchema, schemaPath, outField, false);
      if (outField.memberData) {
        setObjSchemaValueInternal(me.memberDataSchema, schemaPath, outField, false);
      } else {
        setObjSchemaValueInternal(me.sharedDataSchema, schemaPath, outField, false);
      }

      me.fields[field.fieldPath.join('/')] = outField;
    }

    for (let i = 0; i < fieldDescs.length; ++i) {
      addField(fieldDescs[i]);
    }

    const needMembership = this.type === 'personal' || this.type === 'shared';
    const needExistence = this.type === 'global' || this.type === 'subscription' || this.type === 'shared';

    // insert dummy fields in where we need sql tables but they have no fields
    function insertDummyField(sqlPath: string[]) {
      const memberData = sqlPath[0] === 'account';
      const dummyField: InField = {
        name: memberData ? 'mdummy' : 'sdummy',
        type: Types.INT_NULLABLE,
        fieldPath: state.fieldPath.slice(0),
        sqlPath: sqlPath.slice(memberData ? 1 : 0),
        sqlNamePrefix: [],
        parallelStore: false,
        serverOnly: false,
        eachMember: false,
        memberData: memberData,
        noFeed: true,
        deprecated: false,
      };

      for (let i = memberData ? 2 : 1; i < sqlPath.length; ++i) {
        const sqlTableName = sqlPath[i];
        dummyField.fieldPath = dummyField.fieldPath.concat(sqlTableName.split('_'));
        dummyField.fieldPath.push('$' + sqlTableName + 'ID');
      }
      dummyField.fieldPath.push(dummyField.name);
      addField(dummyField);
    }

    for (const sqlPath in sqlPaths) {
      const sqlPath_ = sqlPath.split('/');
      const hasAccount = sqlPath_[0] === 'account';
      for (let j = hasAccount ? 2 : 1; j < sqlPath_.length; ++j) {
        const subPath = sqlPath_.slice(0, j);
        if (!sqlPaths[subPath.join('/')]) {
          insertDummyField(subPath);
        }
      }
    }
    if (needMembership) {
      const sqlPath = ['account'];
      if (this.isRootMap) {
        sqlPath.push(this.name);
      }
      if (!sqlPaths[sqlPath.join('/')]) {
        insertDummyField(sqlPath);
      }
    }
    if (needExistence) {
      const sqlPath: string[] = [];
      if (this.isRootMap) {
        sqlPath.push(this.name);
      }
      if (!sqlPaths[sqlPath.join('/')]) {
        insertDummyField(sqlPath);
      }
    }

    if (needMembership) {
      // determine membership sql table
      for (const sqlTableName in this.sqlTables) {
        const sqlTable = this.sqlTables[sqlTableName];
        if (sqlTable.isParallelStore) {
          continue;
        }
        const primaryKeys = sqlTable.primaryKeys;
        if (primaryKeys[0] !== 'accountID') {
          continue;
        }
        if (primaryKeys.length === 2) {
          this.membershipSqlTableName = sqlTableName;
        } else if (primaryKeys.length === 1 && !this.isRootMap) {
          this.membershipSqlTableName = sqlTableName;
          break;
        }
      }

      if (!this.membershipSqlTableName) {
        tableError(name, 'unable to determine membership sql table');
      }
      this.primarySqlTableName = this.membershipSqlTableName;
    }

    if (needExistence) {
      this.primarySqlTableName = 'sketch' + this.name;
    }

    Object.freeze(this.fields);
  }

  getSchema(path?: string[]) {
    if (path) {
      return ObjSchema.getSchemaForPath(this.objSchema, path);
    }
    return this.objSchema;
  }

  getServerSchema(path?: string[]) {
    if (path) {
      return ObjSchema.getSchemaForPath(this.serverObjSchema, path);
    }
    return this.serverObjSchema;
  }
}

export function buildRowKey(sqlTable: SketchTypes.SqlTable, keyValues: StashOf<string>) {
  const rowKey = [sqlTable.name];
  for (let i = 0; i < sqlTable.primaryKeys.length; ++i) {
    rowKey.push(keyValues[sqlTable.primaryKeys[i]]);
  }
  return rowKey.join('/');
}

function addFilteredField(sketchTable: SketchTable, filteredFields: StashOf<Field>, fieldFilter: FieldFilter, fieldPath, isInMask) {
  const field = sketchTable.fields[fieldPath];

  if (!field) {
    return 'field "' + fieldPath + '" not found';
  }

  if (field.serverOnly && !fieldFilter.allowServerData) {
    if (isInMask) { return 'field "' + field.name + '" is not serverOnly and only server data is allowed for this call'; }
    return;
  }
  if (field.memberData && !fieldFilter.allowMemberData && !fieldFilter.memberDataOnly) {
    if (isInMask) { return 'memberdata field "' + field.name + '" is not allowed for this call'; }
    return;
  }
  if (!field.memberData && fieldFilter.memberDataOnly) {
    if (isInMask) { return 'field "' + field.name + '" is not memberdata and only member data is allowed for this call'; }
    return;
  }

  filteredFields[fieldPath] = field;
}

function addFromFieldMaskInternal(
  sketchTable: SketchTable,
  filteredFields: StashOf<Field>,
  fieldFilter: FieldFilter,
  rootFieldPath: string,
  fieldName: string,
  maskVal: Mask,
) {
  if (fieldName === '_ids') {
    fieldName = sketchTable.mapPaths[rootFieldPath.slice(0, -1)];
    if (maskVal === 1) {
      return addFromFieldMask(sketchTable, filteredFields, fieldFilter, rootFieldPath + fieldName + '/', maskVal);
    }
  }
  const fieldPath = rootFieldPath + fieldName;
  if (!maskVal) {
    // recursion as specified by fieldFilter.recursiveFetch
    return addAllFieldsBelowPath(sketchTable, filteredFields, fieldFilter, fieldPath + '/', fieldFilter.recursiveFetch);
  }
  if (typeof maskVal === 'object') {
    // masked recursion
    return addFromFieldMask(sketchTable, filteredFields, fieldFilter, fieldPath + '/', maskVal);
  }
  if (maskVal === '*') {
    // full recursion
    return addAllFieldsBelowPath(sketchTable, filteredFields, fieldFilter, fieldPath + '/', true);
  }
  // single field
  return addFilteredField(sketchTable, filteredFields, fieldFilter, fieldPath, true);
}

function addFromFieldMask(
  sketchTable: SketchTable,
  filteredFields: StashOf<Field>,
  fieldFilter: FieldFilter,
  rootFieldPath: string,
  fieldMask: Mask,
) {
  if (fieldMask === 1 || fieldMask === '*') {
    return addFromFieldMaskInternal(sketchTable, filteredFields, fieldFilter, rootFieldPath, 'id', 1);
  }

  for (const fieldName in fieldMask) {
    const maskVal = fieldMask[fieldName];
    const err = addFromFieldMaskInternal(sketchTable, filteredFields, fieldFilter, rootFieldPath, fieldName, maskVal);
    if (err) { return err; }
  }
}

function addAllFieldsBelowPath(
  sketchTable: SketchTable,
  filteredFields: StashOf<Field>,
  fieldFilter: FieldFilter,
  rootFieldPath: string,
  allowRecursive?: boolean,
) {
  for (const fieldPath in sketchTable.fields) {
    if (!StringUtils.startsWith(fieldPath, rootFieldPath) && (fieldPath + '/') !== rootFieldPath) {
      continue;
    }

    if (!allowRecursive) {
      const subPath = fieldPath.slice(rootFieldPath.length);
      const firstSlash = subPath.indexOf('/$');
      if (firstSlash >= 0) {
        continue;
      }
    }

    const err = addFilteredField(sketchTable, filteredFields, fieldFilter, fieldPath, false);
    if (err) { return err; }
  }
}

export function applyFieldFilter(sketchTable: SketchTable, rootFieldPath: string, fieldFilter: FieldFilter) {
  // filter fields down to the set we need for the query
  const filteredFields = {};
  let fieldMask = fieldFilter.fieldMask;

  const rootFieldPathExact = rootFieldPath.slice(0, -1);
  const isMapPath = sketchTable.mapPaths.hasOwnProperty(rootFieldPathExact);
  let err;

  if (isMapPath && (!fieldMask || !(fieldMask as any)._ids)) {
    // allow one level deep recursion
    fieldMask = {
      _ids: fieldMask!,
    };
  }

  if (sketchTable.fields[rootFieldPathExact]) {
    // requesting a single field value
    err = addFilteredField(sketchTable, filteredFields, fieldFilter, rootFieldPathExact, true);
  } else if (fieldMask === '*') {
    err = addAllFieldsBelowPath(sketchTable, filteredFields, fieldFilter, rootFieldPath, true);
  } else if (fieldMask) {
    err = addFromFieldMask(sketchTable, filteredFields, fieldFilter, rootFieldPath, fieldMask);
  } else {
    err = addAllFieldsBelowPath(sketchTable, filteredFields, fieldFilter, rootFieldPath, fieldFilter.recursiveFetch);
  }

  return err || filteredFields;
}

export function getAccountVerifyFieldPath(sketchTable: SketchTable, rootFieldPath: string) {
  const rootFieldPathExact = rootFieldPath.slice(0, -1);
  let field;

  if (sketchTable.fields[rootFieldPathExact]) {
    // requesting a single field value
    field = sketchTable.fields[rootFieldPathExact];
    if (field.eachMember) {
      return rootFieldPathExact;
    }
  } else {
    for (const fieldPath in sketchTable.fields) {
      if (!StringUtils.startsWith(fieldPath, rootFieldPath) && (fieldPath + '/') !== rootFieldPath) {
        continue;
      }
      field = sketchTable.fields[fieldPath];
      if (field.eachMember) {
        return fieldPath;
      }
    }
  }

  return null;
}


export function EACH_MEMBER(shape): Types.SchemaMapNode {
  return { _ids: shape, _eachMember: 1 };
}

export function ACCOUNT_MAP(shape): Types.SchemaMapNode {
  return { _ids: shape, _accountKeys: 1 };
}

export function PERSONA_MAP(shape): Types.SchemaMapNode {
  return { _ids: shape, _personaKeys: 1 };
}


function defineTable(tableName: string, descriptor: SketchTypes.SchemaRoot, tableType: TableType) {
  if (gSqlInited) {
    throw new Error('Sketch: defineTable called on table "' + tableName + '" after initSqlTables');
  }

  if (getTable(tableName)) {
    tableError(tableName, 'table aleady exists');
  }

  gAllTables[tableName] = new SketchTable(tableName, descriptor, tableType);
  gAllTableNames.push(tableName);
  return getTable(tableName)!;
}

export function defineSharedTable(tableName: string, descriptor: SketchTypes.SchemaRoot) {
  return defineTable(tableName, descriptor, 'shared');
}

export function definePersonalTable(tableName: string, descriptor: SketchTypes.SchemaRoot) {
  return defineTable(tableName, descriptor, 'personal');
}

export function defineSubscriptionTable(tableName: string, descriptor: SketchTypes.SchemaRoot) {
  return defineTable(tableName, descriptor, 'subscription');
}

export function defineGlobalTable(tableName: string, descriptor: SketchTypes.SchemaRoot) {
  return defineTable(tableName, descriptor, 'global');
}

export function addIndex(sqlTable: SketchTypes.SqlTable, indexFields: StashOf<1>, indexParams?: SketchTypes.IndexParams) {
  if (gSqlInited) {
    throw new Error('Sketch: addIndex called on table "' + sqlTable.name + '" after initSqlTables');
  }

  for (const key in indexFields) {
    if (!sqlTable.schema.hasOwnProperty(key)) {
      throw new Error('Sketch: addIndex called on table "' + sqlTable.name + '" with a field "' + key + '" which is not in the table');
    }
  }

  sqlTable.indexes.push([ indexFields, indexParams || {} ]);
}

export function getTable(tableNameOrPath: string[]|string, isRequired: true): SketchTable;
export function getTable(tableNameOrPath: string[]|string, isRequired: false): SketchTable | null;
export function getTable(tableNameOrPath: string[]|string): SketchTable | null;
export function getTable(tableNameOrPath, isRequired?) {
  const tableName = Array.isArray(tableNameOrPath) ? tableNameOrPath[0] : tableNameOrPath;
  if (!gAllTables.hasOwnProperty(tableName)) {
    if (isRequired) {
      throw new Error('Sketch: table "' + tableName + '" does not exist');
    }
    return null;
  }
  return gAllTables[tableName];
}

export function getTableList() {
  return gAllTableNames;
}

export function initSqlTables(setupTableFunc: (table: SketchTypes.SqlTable) => SketchTypes.SqlTable) {
  gSqlInited = true;

  for (const tableName in gAllTables) {
    const table = gAllTables[tableName];
    if (!table.isServerVisible) {
      continue;
    }
    for (const sqlTableName in table.sqlTables) {
      table.sqlTables[sqlTableName] = setupTableFunc(table.sqlTables[sqlTableName]);
    }
  }
}

export function resetTables() {
  // for tests
  for (const k of Object.keys(gAllTables)) {
    delete gAllTables[k];
  }
}

export const MAP = ObjSchema.MAP;
