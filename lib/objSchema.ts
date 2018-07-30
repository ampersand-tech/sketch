/**
 * Copyright 2015-present Ampersand Technologies, Inc.
 */

import * as Types from './types';

import * as JsonUtils from 'amper-utils/dist2017/jsonUtils';
import * as ObjUtils from 'amper-utils/dist2017/objUtils';
import { StashOf } from 'amper-utils/dist2017/types';


export const VALIDATE_EXISTS = 0;
export const VALIDATE_TYPES = 1;
export const VALIDATE_ALL = 2;
export const VALIDATE_ALL_AND_FILL_DEFAULTS = 3;


export function MAP<T>(shape: T) {
  return { _ids: shape };
}

export function ARRAY_OF<T>(shape: T) {
  return { _idxs: shape };
}

export function descendSchema(schema, key) {
  if (!schema) {
    return undefined;
  }
  return schema._ids || schema._idxs || schema[key];
}


export function validateSchema(schemaNode: Types.Schema, path?: string) {
  if (!ObjUtils.isObject(schemaNode) || Object.keys(schemaNode).length === 0) {
    throw new Error('schema error: for property "' + path + '" invalid node: ' + schemaNode);
  } else if (Types.isRegisteredType(schemaNode)) {
    // looks like a valid type.
    return;
  }

  for (let k in schemaNode) {
    // special field, skip
    if (k === '_nullable' || k === '_eachMember' || k === '_accountKeys' || k === '_personaKeys') {
      continue;
    }
    let s = schemaNode[k];
    let p = (path ? path + '/' : '') + k;
    validateSchema(s, p);
  }
}

export function getDefaultValuesForSchema(schema: Types.Schema|null, nonNullable = false): any {
  if (!schema) {
    return undefined;
  }
  if (Types.isType(schema)) {
    if (schema._nullable && nonNullable) {
      return schema._getDefaultValueNonNullable();
    }
    return schema._getDefaultValue();
  }
  if (!Types.isSchemaMapNode(schema) && !Types.isSchemaArrayNode(schema) && schema._nullable && !nonNullable) {
    return null;
  }

  let val = Types.isSchemaArrayNode(schema) ? [] : {};
  if (!Types.isSchemaMapNode(schema) && !Types.isSchemaArrayNode(schema)) {
    for (let key in schema) {
      if (!Types.isTypeMetaProperty(schema, key)) {
        val[key] = getDefaultValuesForSchema(schema[key]);
      }
    }
  }
  return val;
}

export function getSchemaForPath(rootSchema: Types.Schema, keys: string[]): Types.Schema|null {
  let schema = rootSchema;
  for (let i = 0; i < keys.length; ++i) {
    let key = keys[i];
    if (Types.isSchemaMapNode(schema)) {
      schema = schema._ids;
    } else if (Types.isSchemaArrayNode(schema)) {
      schema = schema._idxs;
    } else if (schema[key] && typeof schema[key] === 'object') {
      schema = schema[key];
    } else {
      return null;
    }
  }
  return schema;
}

export function validateFields(
  rootSchema: Types.Schema,
  keys: string[],
  fields: any,
  typeChecking: number,
  ignoredProps?: StashOf<number> | null,
  tablesModified?: StashOf<number> | null,
  disallowNewFields?: boolean,
) {
  if (typeof typeChecking !== 'number') {
    return 'bad value for typeChecking param passed to validateFields';
  }

  let schema = getSchemaForPath(rootSchema, keys);
  if (!schema) {
    // this is no longer an error, allows feed to apply updates that are not in our schema yet (FutureFeed)
    return null; //'path does not exist in schema: ' + keys.join('/');
  }

  if (!fields) {
    if (typeChecking && !Types.isNullable(schema)) {
      return 'null value found for non-nullable path ' + keys.join('/');
    }
    return null;
  }

  let foundFields: string[] = [];

  for (let propName in fields) {
    if (propName[0] === '_') {
      // ignore _action and other "hidden" props
      continue;
    }
    if (ignoredProps && ignoredProps[propName] === 1) {
      continue;
    }

    let fieldValue = fields[propName];
    let fieldIsObject = fieldValue && ObjUtils.isObject(fieldValue);
    let fieldIsArray = fieldValue && Array.isArray(fieldValue);
    const subSchema = descendSchema(schema, propName);

    if ((fieldIsObject || fieldIsArray) && !Types.isType(subSchema)) {
      if (subSchema && subSchema._ids && !fieldIsObject) {
        return 'wrong type found at ' + keys.concat([propName]).join('/') + ' : ' + fieldValue + ' should be an object';
      }
      if (subSchema && subSchema._idxs && !fieldIsArray) {
        return 'wrong type found at ' + keys.concat([propName]).join('/') + ' : ' + fieldValue + ' should be an array';
      }
      let subKeys = keys.concat([propName]);
      let err = validateFields(rootSchema, subKeys, fieldValue, typeChecking, ignoredProps, tablesModified, disallowNewFields);
      if (err) {
        return err;
      }
      foundFields.push(propName);
    } else if (!subSchema) {
      // by default, this is no longer an error, allows feed to apply updates that are not in our schema yet (FutureFeed)
      if (disallowNewFields) {
        return 'path does not exist in schema: ' + keys.concat([propName]).join('/') + ' schema: ' + JsonUtils.safeStringify(schema)
          + ' fields: ' + JsonUtils.safeStringify(fields);
      }
    } else if (typeChecking) {
      let desiredType = subSchema;
      if (!Types.validateType(fieldValue, desiredType, false, !disallowNewFields)) {
        return 'wrong type found at ' + keys.concat([propName]).join('/') + ' : ' + fieldValue + ' should be ' + desiredType;
      }
      foundFields.push(propName);
    }
  }

  if (typeChecking === VALIDATE_ALL || typeChecking === VALIDATE_ALL_AND_FILL_DEFAULTS) {
    for (let propName in schema) {
      if (propName[0] !== '_' && foundFields.indexOf(propName) < 0) {
        if (typeChecking === VALIDATE_ALL_AND_FILL_DEFAULTS) {
          // upgrading to new schema, fill in default values for new fields (PastFeed)
          fields[propName] = getDefaultValuesForSchema(schema[propName]);

          // mark table as modified so dbModify will write it back out to disk
          if (tablesModified) {
            tablesModified[keys[0]] = 1;
          }
        } else {
          return 'missing value for path: ' + keys.concat([propName]).join('/');
        }
      }
    }
  }

  return null;
}
