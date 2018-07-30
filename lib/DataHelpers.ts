/**
 * Copyright 2015-present Ampersand Technologies, Inc.
 */

import * as TableDef from './TableDef';
import * as Types from './types';

import * as ObjUtils from 'amper-utils/dist2017/objUtils';
import { Stash, StashOf } from 'amper-utils/dist2017/types';

interface FieldFilter {
  allowMemberData?: boolean;
  memberDataOnly?: boolean;
  allowServerData?: boolean;
}

export function isFieldDesc(fieldDesc?: Stash) {
  if (fieldDesc && fieldDesc.sqlTableName && fieldDesc.type && fieldDesc.type._validateType) {
    return true;
  }
  return false;
}

export function schemaIsMap(schema: Types.Schema) {
  const schemaKeys = Object.keys(schema);
  return schemaKeys.length === 1 && schemaKeys[0][0] === '$';
}

export function schemaPassesFilter(schema: Types.Schema, fieldFilter: FieldFilter) {
  const isMap = schemaIsMap(schema);

  for (const key in schema) {
    const schemaValue = schema[key];

    if (isMap) {
      if (schemaPassesFilter(schemaValue, fieldFilter)) {
        return true;
      }
    } else if (isFieldDesc(schemaValue)) {
      if (fieldFilter.memberDataOnly && !schemaValue.memberData) {
        continue;
      }
      if (schemaValue.memberData && !fieldFilter.allowMemberData && !fieldFilter.memberDataOnly) {
        continue;
      }
      if (schemaValue.serverOnly && !fieldFilter.allowServerData) {
        continue;
      }
      if (schemaValue.type._nullable) {
        continue;
      }
      return true;
    } else if (schemaPassesFilter(schemaValue, fieldFilter)) {
      return true;
    }
  }

  return false;
}

export function isValidKey(key: string) {
  return key && key.indexOf('/') < 0;
}

export function walkPathForIds(
  schema: Types.Schema,
  path: string[],
  idValues?: StashOf<string>,
  idPath?: string[],
  fieldPath?: string[],
): Stash|null {
  if (!path.length) {
    return schema;
  }

  const p = path[0];
  path = path.slice(1);
  if (schema[p]) {
    if (fieldPath) {
      fieldPath.push(p);
    }
    return walkPathForIds(schema[p], path, idValues, idPath, fieldPath);
  }

  const schemaKeys = Object.keys(schema);
  if (schemaKeys.length !== 1 || schemaKeys[0][0] !== '$') {
    return null;
  }

  const idName = schemaKeys[0].slice(1);
  if (idValues) {
    idValues[idName] = p;
  }
  if (idPath) {
    idPath.push(idName);
    idPath.push(p);
  }
  if (fieldPath) {
    fieldPath.push('$' + idName);
  }
  return walkPathForIds(schema['$' + idName], path, idValues, idPath, fieldPath);
}

function gatherDefaultValues(schema: Types.Schema, data: Stash, idValues: StashOf<string>, clientOnly: boolean) {
  for (const key in schema) {
    const schemaValue = schema[key];
    if (key[0] === '$') {
      const idName = key.slice(1);
      if (idValues.hasOwnProperty(idName)) {
        const idValue = idValues[idName];
        data[idValue] = data[idValue] || {};
        gatherDefaultValues(schemaValue, data[idValue], idValues, clientOnly);
      }
    } else if (schemaValue.sqlTableName && schemaValue.type && schemaValue.type._validateType) {
      if (!schemaValue.noFeed && (!clientOnly || !schemaValue.serverOnly)) {
        if (!data.hasOwnProperty(key)) {
          if (key === 'id') {
            data[key] = idValues[schemaValue.sqlFieldName];
          } else if (!schemaValue.memberData || idValues.accountID) {
            data[key] = schemaValue.type._getDefaultValue();
          }
        }
      }
    } else if (!clientOnly || schemaPassesFilter(schemaValue, { allowServerData: false, allowMemberData: true })) {
      data[key] = data[key] || {};
      gatherDefaultValues(schemaValue, data[key], idValues, clientOnly);
    }
  }
}

export function populateDefaultValues(schema: Types.Schema, data: Stash, idValues: StashOf<string>, path: string[], clientOnly: boolean) {
  idValues = ObjUtils.clone(idValues);
  const subschema = walkPathForIds(schema, path, idValues);
  if (subschema) {
    gatherDefaultValues(subschema, data, idValues, clientOnly);
  }
}

export function clientDataForCreate(ctx: Context, path: string[]) {
  const table = TableDef.getTable(path, true);

  const data: Stash = {};
  populateDefaultValues(table.memberDataSchema, data, { accountID: ctx.user.id }, path.slice(1), false);
  populateDefaultValues(table.sharedDataSchema, data, {}, path.slice(1), false);
  return data;
}

export function clientDataForMembership(path: string[], accountID: AccountID) {
  const table = TableDef.getTable(path, true);

  if (table.type === 'global' || table.type === 'subscription') {
    throw new Error('cannot call clientDataForMembership on table ' + table.name);
  }

  const data: Stash = {};
  populateDefaultValues(table.memberDataSchema, data, { accountID: accountID }, path.slice(1), false);
  return data;
}

export function serverDataForCreate(_ctx: Context, path: string[]) {
  const table = TableDef.getTable(path, true);

  const data: Stash = {};
  populateDefaultValues(table.sharedDataSchema, data, {}, path.slice(1), false);
  populateDefaultValues(table.memberDataSchema, data, {}, path.slice(1), false);
  return data;
}
