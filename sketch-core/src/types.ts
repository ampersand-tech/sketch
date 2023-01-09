/**
* Copyright 2015-present Ampersand Technologies, Inc.
*/

import { safeStringify, safeParse } from 'amper-utils/dist/jsonUtils';

export const IDSTR_LENGTH = 65;
export const SHORTSTR_LENGTH = 191;

type Basic = number | string | boolean;
type ValidateFunc = (value: any, valType: any, futureFeed?: boolean) => boolean;
type ToSqlFunc = (val: any, valType?: string) => Basic;
type FromSqlFunc = (val: Basic, valType?: Basic) => any;

interface SubTypeParams {
  sqlTypeName: string;
  caseInsensitive: any;
  defaultValue: any;
  defaultValueNonNullable: any;
  validateFunc: ValidateFunc;
  maxStrLength?: number;
  toSqlFunc?: ToSqlFunc;
  fromSqlFunc?: FromSqlFunc;
  fieldOverride: {
    _nullable?: boolean;
    _autoIncrement?: boolean;
  };
}

export interface Type {
  _origType: Type | null;
  _validateType: ValidateFunc;
  _getDefaultValue: () => any;
  _getDefaultValueNonNullable: () => any;
  _hasCustomDefaultValue: boolean;
  _nullable: boolean;
  _autoIncrement: boolean;
  _maxStrLength?: number;
  _sqlTypeName: string;
  _caseInsensitive: boolean;
  _isEnum: boolean;
  _toSql: (val: any) => Basic;
  _fromSql: (val: Basic) => any;
  toString: () => string;
  toJSON: () => string;
}

interface TypeTruncatedString extends Type {
  truncate: (str: string) => string;
}

export function isType(leaf: Schema | null | undefined): leaf is Type {
  if (!leaf) {
    return false;
  }
  return leaf.hasOwnProperty('_validateType');
}

export function isSchemaMapNode(leaf: Schema | null | undefined): leaf is SchemaMapNode {
  if (!leaf) {
    return false;
  }
  return leaf.hasOwnProperty('_ids');
}

export function isSchemaArrayNode(leaf: Schema | null | undefined): leaf is SchemaArrayNode {
  if (!leaf) {
    return false;
  }
  return leaf.hasOwnProperty('_idxs');
}

export function isTypeMetaProperty(_node: Schema, key: string): _node is Schema {
  return (key && key[0]) === '_';
}

// {foo: type} or {_ids: type} but not both
export interface SchemaInternalNode {
  _nullable?: boolean;
  [subType: string]: (Schema | undefined | boolean); // index signature must include type of all named types above, probably a bug
}

export interface SchemaMapNode {
  _ids: SchemaInternalNode;
  _eachMember?: 1;
  _accountKeys?: 1;
  _personaKeys?: 1;
}

export interface SchemaArrayNode {
  _idxs: SchemaInternalNode;
}

export type Schema = SchemaMapNode | SchemaInternalNode | Type | SchemaArrayNode;

// note: not recursive atm, but will match all fields of a flat mask
export function interfaceOf<T>(schema: { [k in keyof T]: any }) {
  return !!schema;
}

let gTypes: Type[] = [];

function defaultToSql<T>(val: T): T {
  return val;
}

function defaultFromSql<T>(val: T): T {
  return val;
}

function validateString(value, valType) {
  // tslint:disable-next-line:no-invalid-this
  return valType === 'string' && (!this._maxStrLength || value.length <= this._maxStrLength);
}

function validateStringLowerCase(value, valType) {
  // tslint:disable-next-line:no-invalid-this
  return valType === 'string' && (!this._maxStrLength || value.length <= this._maxStrLength) && value.toLowerCase() === value;
}

function truncateString(str) {
  // tslint:disable-next-line:no-invalid-this
  return this._maxStrLength ? str.slice(0, this._maxStrLength) : str;
}

function blobToSql(val) {
  return val;
}

function blobFromSql(val) {
  // tslint:disable-next-line:no-invalid-this
  if (!this._nullable && (val === null || val === undefined)) {
    // tslint:disable-next-line:no-invalid-this
    return this._getDefaultValue();
  }
  return val;
}

function objToSql(val) {
  return safeStringify(val);
}

function objFromSql(val) {
  return safeParse(val);
}

function addSubType(typeName: string, params: SubTypeParams) {
  let subType: Type = {
    _origType: null,
    _validateType: params.validateFunc,
    _getDefaultValue: function () {
      return params.defaultValue;
    },
    _getDefaultValueNonNullable: function () {
      return params.defaultValueNonNullable;
    },
    _hasCustomDefaultValue: false,
    _nullable: params.fieldOverride._nullable || false,
    _autoIncrement: params.fieldOverride._autoIncrement || false,
    _maxStrLength: params.maxStrLength || 0,
    _sqlTypeName: params.sqlTypeName,
    _caseInsensitive: params.caseInsensitive,
    _isEnum: false,
    _toSql: params.toSqlFunc || defaultToSql,
    _fromSql: params.fromSqlFunc || defaultFromSql,
    toString: function () {
      return typeName;
    },
    toJSON: function () {
      return typeName;
    },
  };
  gTypes.push(subType);
  exports[typeName] = subType;

  if (Array.isArray(params.defaultValue) || typeof params.defaultValue === 'object') {
    exports[typeName]._getDefaultValue = function () {
      return JSON.parse(JSON.stringify(params.defaultValue)); // @allowJsonFuncs
    };
  }
  if (Array.isArray(params.defaultValueNonNullable) || typeof params.defaultValueNonNullable === 'object') {
    exports[typeName]._getDefaultValueNonNullable = function () {
      return JSON.parse(JSON.stringify(params.defaultValueNonNullable)); // @allowJsonFuncs
    };
  }
}

function addType(
  typeName: string,
  caseInsensitive: boolean,
  defaultValue: any,
  validateFunc: ValidateFunc,
  maxStrLength: number = 0,
  toSqlFunc?: ToSqlFunc,
  fromSqlFunc?: FromSqlFunc,
) {
  addSubType(typeName, {
    sqlTypeName: typeName,
    caseInsensitive: caseInsensitive,
    defaultValue: defaultValue,
    defaultValueNonNullable: defaultValue,
    validateFunc: validateFunc,
    maxStrLength: maxStrLength,
    toSqlFunc: toSqlFunc,
    fromSqlFunc: fromSqlFunc,
    fieldOverride: {},
  });

  addSubType(typeName + '_NULLABLE', {
    sqlTypeName: typeName,
    caseInsensitive: caseInsensitive,
    defaultValue: null,
    defaultValueNonNullable: defaultValue,
    validateFunc: validateFunc,
    maxStrLength: maxStrLength,
    toSqlFunc: toSqlFunc,
    fromSqlFunc: fromSqlFunc,
    fieldOverride: { _nullable: true },
  });

  if (typeName === 'INT') {
    addSubType(typeName + '_AUTO_INCREMENT', {
      sqlTypeName: typeName,
      caseInsensitive: caseInsensitive,
      defaultValue: null,
      defaultValueNonNullable: defaultValue,
      validateFunc: validateFunc,
      maxStrLength: maxStrLength,
      toSqlFunc: toSqlFunc,
      fromSqlFunc: fromSqlFunc,
      fieldOverride: { _autoIncrement: true },
    });
  }
}

export declare const OBJECT: Type;
export declare const OBJECT_NULLABLE: Type;
function isObject(value, valType) {
  return valType === 'object' && !!value && !Array.isArray(value);
}
addType('OBJECT', false, {}, isObject);

export declare const ARRAY: Type;
export declare const ARRAY_NULLABLE: Type;
addType('ARRAY', false, [], function isArray(value, _valType) {
  return !!value && Array.isArray(value);
});

export declare const BOOL: Type;
export declare const BOOL_NULLABLE: Type;
addType('BOOL', false, false, function isBool(_value, valType) {
  return valType === 'boolean';
}, 0, function boolToSql(val) {
  return val ? 1 : 0;
}, function boolFromSql(val) {
  return !!val;
});

export declare const NUMBER: Type;
export declare const NUMBER_NULLABLE: Type;
addType('NUMBER', false, 0, function isNumber(value, valType) {
  // must disallow NaN and Infinity because they turn to null during JSON conversion
  return valType === 'number' && isFinite(value);
});

export declare const INT: Type;
export declare const INT_AUTO_INCREMENT: Type;
export declare const INT_NULLABLE: Type;
addType('INT', false, 0, function isInteger(value, valType) {
  return valType === 'number' && isFinite(value) && value === (value | 0);
});

export declare const TIME: Type;
export declare const TIME_NULLABLE: Type;
addType('TIME', false, 0, function isTime(value, valType) {
  return valType === 'number' && isFinite(value) && value === Math.floor(value);
});

export declare const TIME_TZ: Type;
export declare const TIME_TZ_NULLABLE: Type;
addType('TIME_TZ', false, 'epoch', function isTimeWithTimeZone(_value, valType) {
  return valType === 'string';
});

export declare const IDSTR: TypeTruncatedString;
export declare const IDSTR_NULLABLE: Type;
addType('IDSTR', false, '', validateString, IDSTR_LENGTH);
exports.IDSTR.truncate = truncateString;

export declare const ACCOUNTIDSTR: TypeTruncatedString;
export declare const ACCOUNTIDSTR_NULLABLE: Type;
addType('ACCOUNTIDSTR', false, '', validateString, IDSTR_LENGTH);
exports.ACCOUNTIDSTR.truncate = truncateString;

export declare const PERSONAIDSTR: TypeTruncatedString;
export declare const PERSONAIDSTR_NULLABLE: Type;
addType('PERSONAIDSTR', false, '', validateString, IDSTR_LENGTH);
exports.PERSONAIDSTR.truncate = truncateString;

export declare const SHORTSTR: TypeTruncatedString;
export declare const SHORTSTR_NULLABLE: Type;
addType('SHORTSTR', false, '', validateString, SHORTSTR_LENGTH);
exports.SHORTSTR.truncate = truncateString;

export declare const SHORTSTR_LOWERCASE: TypeTruncatedString;
export declare const SHORTSTR_LOWERCASE_NULLABLE: Type;
addType('SHORTSTR_LOWERCASE', false, '', validateStringLowerCase, SHORTSTR_LENGTH);
exports.SHORTSTR_LOWERCASE.truncate = truncateString;
exports.SHORTSTR_LOWERCASE._sqlTypeName = exports.SHORTSTR._sqlTypeName;

export declare const LONGSTR: Type;
export declare const LONGSTR_NULLABLE: Type;
addType('LONGSTR', false, '', validateString, 0, blobToSql, blobFromSql);

export declare const BINSTR: Type;
export declare const BINSTR_NULLABLE: Type;
addType('BINSTR', false, '', validateString, 0, blobToSql, blobFromSql);

export declare const JSONBLOB: Type;
export declare const JSONBLOB_NULLABLE: Type;
addType('JSONBLOB', false, '', function (_value, _valType) {
  return true;
}, 0, objToSql, objFromSql);
exports.JSONBLOB._sqlTypeName = exports.LONGSTR._sqlTypeName;
exports.JSONBLOB_NULLABLE._sqlTypeName = exports.LONGSTR_NULLABLE._sqlTypeName;

export declare const STRING: Type;
export declare const STRING_NULLABLE: Type;
addType('STRING', false, '', validateString);

export declare const STRING_ARRAY: Type;
export declare const STRING_ARRAY_NULLABLE: Type;
addType('STRING_ARRAY', false, [], function isStringArray(value, _valType) {
  if (!Array.isArray(value)) {
    return false;
  }
  for (let i = 0; i < value.length; ++i) {
    if (typeof value[i] !== 'string') {
      return false;
    }
  }
  return true;
});

export declare const FUNCTION: Type;
export declare const FUNCTION_NULLABLE: Type;
addType('FUNCTION', false, null, function isFunction(_value, valType) {
  return valType === 'function';
}, 0, function functionToSql() {
  throw new Error('FUNCTION can not be persisted');
}, function functionFromSql() {
  throw new Error('FUNCTION can not be persisted');
});

export function isNullable(type: Schema): boolean {
  return (isSchemaMapNode(type) || isSchemaArrayNode(type)) ? false : !!type._nullable;
}

export function validateType(value, type: Schema, undefinedAllowedNullable?: boolean, futureFeed?) {
  if (value === undefined) {
    if (undefinedAllowedNullable) {
      value = null;
    } else {
      return false;
    }
  }
  if (value === null) {
    return isNullable(type);
  }

  const valType = typeof value;

  if (isType(type)) {
    return type._validateType(value, valType, futureFeed);
  }

  if (isSchemaMapNode(type)) {
    if (!isObject(value, valType)) {
      return false;
    }
    for (let id in value) {
      if (!validateType(value[id], type._ids, true, futureFeed)) {
        return false;
      }
    }
  } else if (isSchemaArrayNode(type)) {
    if (!Array.isArray(value)) {
      return false;
    }
    for (const aValue of value) {
      if (!validateType(aValue, type._idxs, true, futureFeed)) {
        return false;
      }
    }
  } else {
    if (!isObject(value, valType)) {
      return false;
    }
    for (let key in type) {
      if (isTypeMetaProperty(type, key)) {
        continue;
      }
      if (!validateType(value[key], type[key], true, futureFeed)) {
        return false;
      }
    }

    for (let key in value) {
      if (!type.hasOwnProperty(key)) {
        return false;
      }
    }
  }

  return true;
}

function isNullableTypeStr(str: string): boolean {
  return str.slice(-9) === '_NULLABLE';
}

function getBaseType(str: string): string {
  return str.split('_NULLABLE')[0].split('_AUTO_INCREMENT')[0].split('_LOWERCASE')[0];
}

export function checkTypesBackwardsCompatible(newType, lastType) {
  if (!newType) {
    return false;
  }

  if (typeof lastType !== typeof newType) {
    return false;
  }

  if (typeof lastType === 'string') {
    const newTypeNullable = isNullableTypeStr(newType);
    const baseNewType = getBaseType(newType);
    const lastTypeNullable = isNullableTypeStr(lastType);
    const baseLastType = getBaseType(lastType);

    if (lastTypeNullable && !newTypeNullable) {
      return false;
    }

    // check if the new type is the same as the last, or a nullable version of the last
    if (baseNewType === baseLastType) {
      return true;
    }

    // special case for numeric upgrades
    if (baseLastType === 'INT' && (baseNewType === 'NUMBER' || baseNewType === 'TIME')) {
      return true;
    }
    if (baseLastType === 'TIME' && baseNewType === 'NUMBER') {
      return true;
    }

    // special case for string upgrades
    const isShortOrLongStr = baseNewType === 'LONGSTR' || baseNewType === 'SHORTSTR';
    if (baseLastType === 'IDSTR' && (baseNewType === 'ACCOUNTIDSTR' || baseNewType === 'PERSONAIDSTR' || isShortOrLongStr)) {
      return true;
    }
    if (baseLastType === 'PERSONAIDSTR' && (baseNewType === 'IDSTR' || baseNewType === 'ACCOUNTIDSTR' || isShortOrLongStr)) {
      return true;
    }
    if (baseLastType === 'ACCOUNTIDSTR' && (baseNewType === 'IDSTR' || baseNewType === 'PERSONAIDSTR' || isShortOrLongStr)) {
      return true;
    }
    if (baseLastType === 'SHORTSTR' && baseNewType === 'LONGSTR') {
      return true;
    }

    return false;
  }

  if (Array.isArray(lastType) && Array.isArray(newType)) {
    if (lastType[0] !== newType[0]) {
      return false;
    }
    for (let i = 0; i < lastType.length; ++i) {
      if (newType.indexOf(lastType[i]) < 0) {
        return false;
      }
    }
    return true;
  }

  // check old fields
  for (let key in lastType) {
    if (!checkTypesBackwardsCompatible(newType[key], lastType[key])) {
      return false;
    }
  }

  // new fields are ok if they are nullable
  for (let key in newType) {
    if (lastType[key]) {
      continue;
    }
    if (typeof newType[key] !== 'string') {
      return false;
    }
    if (!isNullableTypeStr(newType[key])) {
      return false;
    }
  }

  return true;
}

export function createEnum(enumName: string, values, nullable = false) {
  nullable = !!nullable;

  if (!Array.isArray(values)) {
    // convert object to array of values
    const obj = values;
    values = [];
    for (let key in obj) {
      values.push(obj[key]);
    }
  }

  const res = {
    _origType: null,
    _validateType: function (value, valType, futureFeed?) {
      if (!exports.IDSTR._validateType(value, valType)) {
        return false;
      }
      if (values.indexOf(value) < 0 && !futureFeed) {
        return false;
      }
      return true;
    },
    _getDefaultValue: function () {
      return values[0];
    },
    _getDefaultValueNonNullable: function () {
      return values[0];
    },
    _hasCustomDefaultValue: true,
    _nullable: nullable,
    _autoIncrement: false,
    _sqlTypeName: 'ENUM',
    _caseInsensitive: false,
    _isEnum: true,
    _toSql: defaultToSql,
    _fromSql: defaultFromSql,
    toString: function () {
      return enumName;
    },
    toJSON: function () {
      const v = values.slice(0);
      if (res._nullable) {
        v.push('NULL');
      }
      return v;
    },
  };
  gTypes.push(res);
  return res;
}

export function withDefaultValue(type: Type, defaultValue) {
  if (!validateType(defaultValue, type)) {
    throw new Error('default value does not validate');
  }

  if (type._sqlTypeName === 'LONGSTR' || type._sqlTypeName === 'BINSTR') {
    throw new Error(type._sqlTypeName + ' does not support custom default values');
  }

  if (type._nullable) {
    throw new Error('NULLABLE types cannot have a custom default value, the default will be null');
  }

  return {
    _origType: type._origType || type,
    _validateType: type._validateType,
    _getDefaultValue: function () {
      return defaultValue;
    },
    _getDefaultValueNonNullable: function () {
      return defaultValue;
    },
    _hasCustomDefaultValue: true,
    _nullable: false,
    _autoIncrement: false,
    _maxStrLength: type._maxStrLength,
    _sqlTypeName: type._sqlTypeName,
    _caseInsensitive: type._caseInsensitive,
    _isEnum: type._isEnum,
    _toSql: type._toSql,
    _fromSql: type._fromSql,
    toString: type.toString,
    toJSON: type.toJSON,
  };
}

export function withMaxStringLength(type: Type, maxLength: number): TypeTruncatedString {
  if (type._validateType !== validateString) {
    throw new Error('withMaxStringLength must be called with a string type');
  }

  if (!maxLength || maxLength <= 0 || (type._maxStrLength && maxLength >= type._maxStrLength)) {
    throw new Error('withMaxStringLength called with an invalid maxLength');
  }

  return {
    _origType: type._origType || type,
    _validateType: validateString,
    _getDefaultValue: type._getDefaultValue,
    _getDefaultValueNonNullable: type._getDefaultValueNonNullable,
    _hasCustomDefaultValue: type._hasCustomDefaultValue,
    _nullable: type._nullable,
    _autoIncrement: type._autoIncrement,
    _maxStrLength: maxLength,
    _sqlTypeName: type._sqlTypeName,
    _caseInsensitive: type._caseInsensitive,
    _isEnum: type._isEnum,
    _toSql: type._toSql,
    _fromSql: type._fromSql,
    toString: type.toString,
    toJSON: type.toJSON,
    truncate: truncateString,
  };
}

export function isRegisteredType(type: Schema) {
  if (!isType(type)) {
    return false;
  }
  if (type && type._origType) {
    type = type._origType;
  }
  return gTypes.indexOf(type) >= 0;
}
