/**
* Copyright 2017-present Ampersand Technologies, Inc.
*
* Big TODOs:
* - reads after writes for the same rows need to actually be scheduled after the writes, so they see the modified data
* - need a way to ignore write errors
*
*/

import * as Sketch from './index';
import { AccountTokenCacheInterface, SketchMask } from './SketchTypes';
import * as Types from './types';

import * as JsonUtils from 'amper-utils/dist2017/jsonUtils';
import * as ObjUtils from 'amper-utils/dist2017/objUtils';
import { absurd, Stash, StashOf } from 'amper-utils/dist2017/types';

let AccountTokenCache: AccountTokenCacheInterface | undefined;

type DataPath = (string|DataHandle)[];

interface ProgramSettings {
  canRunOnClient?: boolean;
  runAsAdmin?: boolean;
}

class ExecutionContext {
  prog: SketchProgram;
  ctx: Context;
  dataHandleValues: Stash;
  dataHandleErrors: StashOf<string|Error>;
  accountsModified: StashOf<1>;
  debug: boolean;
}

type WriteOperation =
  'insertClientData' |
  'initializeClientData' |
  'initializeClientPath' |
  'updateClientData' |
  'replaceClientData' |
  'removeClientData' |
  'insertServerData' |
  'initializeServerData' |
  'initializeServerPath' |
  'updateServerData' |
  'replaceServerData' |
  'removeServerData' |
  'addMember' |
  'removeMember'
;

type ReadOperation =
  'getClientData' |
  'getServerData' |
  'lookupUser'
;

interface ProgramOpShared {
  conditions: DataHandle[];
  user: DataHandle | undefined;
}

interface DataFetch extends ProgramOpShared {
  opType: 'DataFetch';
  readType: ReadOperation;
  handle: DataHandle;
  path: DataPath;
  mask?: SketchMask;
  accountID?: DataHandle;
}

interface DataWrite extends ProgramOpShared {
  opType: 'DataWrite';
  writeType: WriteOperation;
  path: DataPath;
  value?: any;
  accountID?: DataHandle;
}

interface ExternalFunc extends ProgramOpShared {
  opType: 'ExternalFunc';
  isSync: boolean;
  func: Function;
  args: any[];
  handle: DataHandle;
}

interface AbortOnError extends ProgramOpShared {
  opType: 'AbortOnError';
  handle?: DataHandle;
  error?: Error;
}

type ProgramOp = DataFetch | DataWrite | ExternalFunc | AbortOnError;


function opToDebugInfo(op: ProgramOp) {
  const ret: Stash = {
    opType: op.opType,
    user: op.user && op.user.dbgName,
    conditions: op.conditions.map((cond) => cond.dbgName),
  };

  switch (op.opType) {
    case 'DataFetch':
      ret.readType = op.readType;
      ret.path = op.path.join('/');
      ret.accountID = op.accountID && op.accountID.toString();
      break;

    case 'DataWrite':
      ret.writeType = op.writeType;
      ret.path = op.path.join('/');
      ret.value = JsonUtils.jsonClone(op.value);
      ret.accountID = op.accountID && op.accountID.toString();
      break;

    case 'ExternalFunc':
      ret.func = op.func.name;
      break;

    case 'AbortOnError':
      if (op.handle) {
        ret.handle = op.handle.dbgName;
      }
      if (op.error) {
        ret.error = op.error;
      }
      break;

    default:
      absurd(op);
  }

  return ret;
}

function evalConditions(ectx: ExecutionContext, op: ProgramOp) {
  for (const condition of op.conditions) {
    let res;
    try {
      res = evalHandles(ectx, condition);
    } catch (err) {
      if (ectx.debug) {
        Log.info(ectx.ctx, `evalHandles failed: ${condition.dbgName}`, {err});
        debugger;
      }
      throw err;
    }
    if (!res) {
      return false;
    }
  }
  return true;
}

async function runDataWrite(ectx: ExecutionContext, op: DataWrite) {
  const ctx = ObjUtils.shallowClone(ectx.ctx) as ServerContext;
  let path: string[];
  let value: any;
  let accountID: AccountID;

  try {
    if (!evalConditions(ectx, op)) {
      return;
    }

    path = evalHandles(ectx, op.path);
    value = evalHandles(ectx, op.value);
    accountID = evalHandles(ectx, op.accountID);
    if (op.user) {
      ctx.user = evalHandles(ectx, op.user);
    }

    if (path[0] === 'account') {
      ectx.accountsModified[ctx.user.id] = 1;
    }

    switch (op.writeType) {
      case 'insertClientData':
        const clientData = Sketch.clientDataForCreate(ctx, path);
        ObjUtils.copyFields(value, clientData);
        return await Sketch.insertClientData(ctx, path, clientData);

      case 'initializeClientData':
        return await Sketch.initializeClientData(ctx, path);

      case 'initializeClientPath':
        return await Sketch.initializeClientPath(ctx, path);

      case 'updateClientData':
        return await Sketch.updateClientData(ctx, path, value);

      case 'replaceClientData':
        return await Sketch.replaceClientData(ctx, path, value);

      case 'removeClientData':
        return await Sketch.removeClientData(ctx, path);

      case 'insertServerData':
        const serverData = Sketch.serverDataForCreate(ctx, path);
        ObjUtils.copyFields(value, serverData);
        return await Sketch.insertServerData(ctx, path, serverData);

      case 'initializeServerData':
        return await Sketch.initializeServerData(ctx, path);

      case 'initializeServerPath':
        return await Sketch.initializeServerPath(ctx, path);

      case 'updateServerData':
        return await Sketch.updateServerData(ctx, path, value);

      case 'replaceServerData':
        return await Sketch.replaceServerData(ctx, path, value);

      case 'removeServerData':
        return await Sketch.removeServerData(ctx, path);

      case 'addMember':
        const memberData = Sketch.clientDataForMembership(path, accountID);
        ObjUtils.copyFields(value, memberData);
        if (op.user) {
          // verify that the user has access to the data
          try {
            await Sketch.getClientData(ctx, path, {});
          } catch (err) {
            if (ErrorUtils.isNotFound(err)) {
              throw new Error('addMember requires the current user to be a member');
            }
            throw err;
          }
        }
        return await Sketch.insertMember(ctx, path, accountID, memberData);

      case 'removeMember':
        if (op.user) {
          // verify that the user has access to the data
          try {
            await Sketch.getClientData(ctx, path, {});
          } catch (err) {
            if (ErrorUtils.isNotFound(err)) {
              throw new Error('removeMember requires the current user to be a member');
            }
            throw err;
          }
        }
        // running as admin/server, no access check required
        return await Sketch.removeMember(ctx, path, accountID);

      default:
        absurd(op.writeType);
        throw new Error('unhandled writeType: ' + op.writeType);
    }
  } catch (err) {
    if (ectx.debug) {
      ctx; path!; value; accountID; op;
      console.log(`${op.writeType}`, {path: path!, value, err});
      debugger;
    }
    throw err;
  }
}

async function runDataFetch(ectx: ExecutionContext, op: DataFetch) {
  const ctx = ObjUtils.shallowClone(ectx.ctx) as ServerContext;
  let path, accountID;

  function onData(data?) {
    if (ectx.debug) {
      ctx; path; accountID; op;
      console.log(`${op.readType}`, {path, accountID, data});
      debugger;
    }
    ectx.dataHandleValues[op.handle.handleKey] = data;
  }

  try {
    if (!evalConditions(ectx, op)) {
      return;
    }

    path = evalHandles(ectx, op.path);
    accountID = evalHandles(ectx, op.accountID);
    if (op.user) {
      ctx.user = evalHandles(ectx, op.user);
    }

    switch (op.readType) {
      case 'getClientData':
        return onData(await Sketch.getClientData(ctx, path, op.mask || null));

      case 'getServerData':
        return onData(await Sketch.getServerData(ctx, path, op.mask || null));

      case 'lookupUser':
        if (!AccountTokenCache) {
          throw new Error('SketchProgram not initialized');
        }
        return onData(await wrap(AccountTokenCache.findByAccountID, ctx, accountID, 'user'));

      default:
        absurd(op.readType);
        throw new Error('unhandled readType: ' + op.readType);
    }
  } catch (err) {
    if (ectx.debug) {
      ctx; path; accountID; op;
      console.log(`${op.readType}`, {path, accountID, err});
      debugger;
    }
    ectx.dataHandleErrors[op.handle.handleKey] = err;
  }
}

async function runExternalFunc(ectx: ExecutionContext, op: ExternalFunc) {
  const ctx = ObjUtils.shallowClone(ectx.ctx) as ServerContext;
  let args;

  function onData(data?) {
    if (ectx.debug) {
      ctx; args; op;
      console.log(`${op.func.name}`, {args, data});
      debugger;
    }
    ectx.dataHandleValues[op.handle.handleKey] = data;
  }

  try {
    if (!evalConditions(ectx, op)) {
      return;
    }

    args = evalHandles(ectx, op.args);
    if (op.user) {
      ctx.user = evalHandles(ectx, op.user);
    }

    args.unshift(ctx);

    if (op.isSync) {
      onData(op.func(...args));
    } else {
      onData(await op.func(...args));
    }
  } catch (err) {
    if (ectx.debug) {
      ctx; args; op;
      console.log(`${op.func.name}`, {args, err});
      debugger;
    }
    ectx.dataHandleErrors[op.handle.handleKey] = err;
  }
}

async function runAbortOnError(ectx: ExecutionContext, op: AbortOnError) {
  if (!evalConditions(ectx, op)) {
    return;
  }
  evalHandles(ectx, op.handle);
  throw op.error;
}

function getHandleValue(ectx: ExecutionContext, handle: DataHandle) {
  if (ectx.dataHandleValues.hasOwnProperty(handle.handleKey)) {
    return ectx.dataHandleValues[handle.handleKey];
  }

  if (ectx.dataHandleErrors.hasOwnProperty(handle.handleKey)) {
    throw ectx.dataHandleErrors[handle.handleKey];
  }

  if (handle.accessorFunc) {
    // note: accessorFunc might throw an exception, and that's ok; we purposefully don't cache
    // the error here and just let it rethrow as needed
    ectx.dataHandleValues[handle.handleKey] = handle.accessorFunc(ectx, ...handle.accessorArgs);
    return ectx.dataHandleValues[handle.handleKey];
  }

  throw `DataHandle ${handle.handleKey} has not been populated yet`;
}


function evalHandles(ectx: ExecutionContext, obj) {
  if (obj instanceof DataHandle) {
    return getHandleValue(ectx, obj);
  }
  if (Array.isArray(obj)) {
    const res: any[] = [];
    for (const elem of obj) {
      res.push(evalHandles(ectx, elem));
    }
    return res;
  }
  if (ObjUtils.isObject(obj)) {
    const res = {};
    for (const key in obj) {
      const val = evalHandles(ectx, obj[key]);
      if (val === undefined) {
        continue;
      }
      if (key[0] === '$') {
        const keyHandle = ectx.prog.getHandleByKey(key);
        res[getHandleValue(ectx, keyHandle)] = val;
      } else {
        res[key] = val;
      }
    }
    return res;
  }

  return obj;
}

function getMaxHandleDepth(prog: ProgramBuilder, maxDepth: number, obj) {
  if (obj instanceof DataHandle) {
    obj.isUsed = true;
    return Math.max(obj.depth, maxDepth);
  }
  if (Array.isArray(obj)) {
    for (const elem of obj) {
      maxDepth = getMaxHandleDepth(prog, maxDepth, elem);
    }
    return maxDepth;
  }
  if (ObjUtils.isObject(obj)) {
    for (const key in obj) {
      if (key[0] === '$') {
        const keyHandle = prog.getHandleByKey(key);
        keyHandle.isUsed = true;
        maxDepth = Math.max(keyHandle.depth, maxDepth);
      }
      maxDepth = getMaxHandleDepth(prog, maxDepth, obj[key]);
    }
    return maxDepth;
  }

  return maxDepth;
}

function deref(ectx: ExecutionContext, handle: DataHandle, name: string|DataHandle) {
  return getHandleValue(ectx, handle)[evalHandles(ectx, name)];
}

function withProps(ectx: ExecutionContext, handle: DataHandle, props: Stash) {
  const obj = ObjUtils.clone(getHandleValue(ectx, handle));
  ObjUtils.copyFields(evalHandles(ectx, props), obj);
  return obj;
}

function cmpEqual(ectx: ExecutionContext, value1, value2) {
  return evalHandles(ectx, value1) === evalHandles(ectx, value2);
}

function cmpNotEqual(ectx: ExecutionContext, value1, value2) {
  return evalHandles(ectx, value1) !== evalHandles(ectx, value2);
}

function invertBool(ectx: ExecutionContext, value) {
  return !evalHandles(ectx, value);
}

function logicalAnd(ectx: ExecutionContext, ...values) {
  const bools = evalHandles(ectx, values);
  for (const b of bools) {
    if (!b) {
      return false;
    }
  }
  return true;
}

function logicalOr(ectx: ExecutionContext, ...values) {
  const bools = evalHandles(ectx, values);
  for (const b of bools) {
    if (b) {
      return true;
    }
  }
  return false;
}

function ignoreError(ectx: ExecutionContext, handle: DataHandle, err: string, defaultValue) {
  let value = defaultValue;
  try {
    value = getHandleValue(ectx, handle);
  } catch (thrownErr) {
    if (ErrorUtils.errorToString(thrownErr, false) !== err) {
      // not the droid we're looking for, rethrow it
      throw thrownErr;
    }
  }
  return value;
}

function hasError(ectx: ExecutionContext, handle: DataHandle, err: string|undefined) {
  try {
    getHandleValue(ectx, handle);
  } catch (thrownErr) {
    if (err === undefined) {
      // looking for any error, return true
      return true;
    }

    if (ErrorUtils.errorToString(thrownErr, false) === err) {
      // error is the one we are looking for, return true
      return true;
    }

    // not the error we were looking for, go ahead and throw it up the chain
    throw thrownErr;
  }

  // no error, return false
  return false;
}

function applyMask(ectx: ExecutionContext, handle: DataHandle, mask: SketchMask) {
  const obj = getHandleValue(ectx, handle);
  return ObjUtils.cloneWithMask(obj, mask, undefined);
}

function keyCount(ectx: ExecutionContext, handle: DataHandle) {
  const obj = getHandleValue(ectx, handle);
  return Object.keys(obj).length;
}

export class DataHandle {
  readonly dbgName: string;
  readonly handleKey: string;
  readonly depth: number = 0;

  private prog: ProgramBuilder;

  readonly accessorFunc;
  readonly accessorArgs;

  isUsed = false;

  constructor(prog: ProgramBuilder, dbgName: string, depth: number, accessor?: any, ...args) {
    this.handleKey = prog.addDataHandle(this);
    this.prog = prog;
    this.dbgName = dbgName;
    this.depth = depth;
    this.accessorFunc = accessor;
    this.accessorArgs = args;

    if (this.accessorArgs) {
      this.depth = getMaxHandleDepth(prog, this.depth, this.accessorArgs);
    }
  }

  toString() {
    return this.dbgName;
  }

  toJSON() {
    return this.dbgName;
  }

  getProperty(name: string|DataHandle) {
    return new DataHandle(this.prog, `${this}.${name}`, 0, deref, this, name);
  }

  withProperties(props: Stash) {
    return new DataHandle(this.prog, `${this}.withProperties()`, 0, withProps, this, props);
  }

  notEqualTo(value: DataHandle|string|number|boolean|null) {
    return new DataHandle(this.prog, `${this}.notEqualTo(${value})`, 0, cmpNotEqual, this, value);
  }

  equalTo(value: DataHandle|string|number|boolean|null) {
    return new DataHandle(this.prog, `${this}.equalTo(${value})`, 0, cmpEqual, this, value);
  }

  not() {
    return new DataHandle(this.prog, `${this}.not()`, 0, invertBool, this);
  }

  and(...values: DataHandle[]) {
    return new DataHandle(this.prog, `${this}.and(${values})`, 0, logicalAnd, this, ...values);
  }

  or(...values: DataHandle[]) {
    return new DataHandle(this.prog, `${this}.or(${values})`, 0, logicalOr, this, ...values);
  }

  ignoreError(err: string, defaultValue?: any) {
    return new DataHandle(this.prog, `${this}.ignoreError(${err})`, 0, ignoreError, this, err, defaultValue);
  }

  hasError(err?: string) {
    return new DataHandle(this.prog, `${this}.hasError(${err})`, 0, hasError, this, err);
  }

  applyMask(mask: SketchMask) {
    return new DataHandle(this.prog, `${this}.applyMask()`, 0, applyMask, this, mask);
  }

  keyCount() {
    return new DataHandle(this.prog, `${this}.keyCount()`, 0, keyCount, this);
  }
}

interface OpInput {
  handle: DataHandle;
  schema: Types.Schema;
  defaultValue: any;
}

interface ParentProgramOptions {
  rootProgram: ProgramBuilder;
  parentProgram: ProgramBuilder;
  userHandle?: DataHandle;
  condition?: DataHandle;
}

type SubprogScope = () => void;

export class ProgramBuilder {
  private rootProgram: ProgramBuilder|undefined;
  private parentProgram: ProgramBuilder|undefined;
  private minDepth: number = 0;
  private maxDepth: number = 0;

  private writeDepth: number = 0;
  private activeProg: ProgramBuilder|undefined;
  private condition: DataHandle|undefined;
  private subPrograms: ProgramBuilder[] = [];

  private permFuncs: Function[] = [];
  private dataHandles: StashOf<DataHandle> = {};
  private dataHandleCount = 0;
  private userHandle: DataHandle|undefined;
  private inputs: StashOf<OpInput> = {};
  private uuids: StashOf<{ handle: DataHandle, optType?: string }> = {};
  private clientTimeHandle: DataHandle|undefined;
  private serverTimeHandle: DataHandle|undefined;
  private outputValue: DataHandle|undefined;
  private operations: StashOf<ProgramOp[]> = {};

  readonly settings: ProgramSettings;

  constructor(settings?: ProgramSettings, parentOpts?: ParentProgramOptions) {
    this.settings = settings || {};

    if (parentOpts) {
      this.rootProgram = parentOpts.rootProgram;
      this.parentProgram = parentOpts.parentProgram;
      this.userHandle = parentOpts.userHandle;
      this.condition = parentOpts.condition;

      this.minDepth = this.parentProgram.minDepth;
      if (this.userHandle) {
        // don't run until userHandle is ready
        this.minDepth = Math.max(this.minDepth, this.userHandle.depth);
        this.updateMaxDepth(this.userHandle.depth);
      }
      if (this.condition) {
        // don't run until condition is ready
        this.minDepth = Math.max(this.minDepth, this.condition.depth);
        this.condition.isUsed = true;
        this.updateMaxDepth(this.condition.depth);
      }

      this.writeDepth = Math.max(this.minDepth, this.parentProgram.writeDepth);
      this.updateMaxDepth(this.writeDepth);
    } else {
      if (this.settings.canRunOnClient && this.settings.runAsAdmin) {
        throw new Error('ProgramBuilder cannot set both canRunOnClient and runAsAdmin');
      }
      if (!this.settings.runAsAdmin) {
        this.userHandle = new DataHandle(this.rootProgram || this, 'ctx.user', 0);
        this.userHandle.isUsed = true;
      }
    }
  }

  private updateMaxDepth(depth: number) {
    this.maxDepth = Math.max(this.maxDepth, depth);
    if (this.parentProgram) {
      this.parentProgram.updateMaxDepth(this.maxDepth);
    }
  }

  private addOperation(depth: number, op: ProgramOp) {
    if (this.condition) {
      op.conditions.unshift(this.condition);
    }
    if (this.rootProgram) {
      this.rootProgram.addOperation(depth, op);
      return;
    }
    this.operations[depth] = this.operations[depth] || [];
    this.operations[depth].push(op);
  }

  private addDataFetch(readType: ReadOperation, path: DataPath, mask?: SketchMask, accountID?: DataHandle): DataHandle {
    const depth = getMaxHandleDepth(this.rootProgram || this, this.minDepth, [path, accountID]);
    const handle = new DataHandle(this.rootProgram || this, `${readType}(${path})`, depth + 1);
    const op: DataFetch = {
      opType: 'DataFetch',
      conditions: [],
      readType,
      user: this.userHandle,
      handle,
      path,
      mask,
      accountID,
    };
    this.addOperation(depth, op);
    this.updateMaxDepth(handle.depth);
    return handle;
  }

  private getWriteDepth(...args) {
    // writeDepth is ascending, so writes happen in the order written
    if (this.parentProgram) {
      // also update parent program write depth
      this.parentProgram.getWriteDepth(args);
    }
    this.writeDepth = getMaxHandleDepth(this.rootProgram || this, this.writeDepth, args);
    const depth = this.writeDepth + 1;
    this.updateMaxDepth(depth);
    return depth;
  }

  addDataHandle(handle: DataHandle): string {
    if (this.rootProgram) {
      return this.rootProgram.addDataHandle(handle);
    }
    const key = '$' + (this.dataHandleCount++).toString();
    this.dataHandles[key] = handle;
    return key;
  }

  getHandleByKey(key: string) {
    const handle = this.dataHandles[key];
    if (!handle) {
      throw new Error('No such handle: ' + key);
    }
    return handle;
  }

  get user(): DataHandle {
    if (this.activeProg) {
      return this.activeProg.user;
    }
    if (!this.userHandle) {
      throw new Error('No active user in this context');
    }
    return this.userHandle;
  }

  get userID(): DataHandle {
    return this.user.getProperty('id');
  }

  getInput(key: string, schema: Types.Schema, defaultValue?: any): DataHandle {
    if (this.rootProgram) {
      return this.rootProgram.getInput(key, schema, defaultValue);
    }
    if (!this.inputs.hasOwnProperty(key)) {
      this.inputs[key] = {
        handle: new DataHandle(this.rootProgram || this, `inputs<${key}>`, 0),
        schema,
        defaultValue,
      };
      this.inputs[key].handle.isUsed = true;
    } else if (schema !== this.inputs[key].schema) {
      throw new Error('getInput called more than once with different schemas');
    }

    return this.inputs[key].handle;
  }

  returnValue(value: DataHandle) {
    if (this.activeProg) {
      this.activeProg.returnValue(value);
      return;
    }
    value.isUsed = true;
    this.updateMaxDepth(value.depth);
    this.outputValue = value;
  }

  returnError(err: string) {
    if (this.activeProg) {
      this.activeProg.returnError(err);
      return;
    }
    const depth = this.minDepth;
    const op: AbortOnError = {
      opType: 'AbortOnError',
      conditions: [],
      user: this.userHandle,
      error: new Error(err),
    };
    this.addOperation(depth, op);
  }

  if(condition: DataHandle, func?: SubprogScope, elseFunc?: SubprogScope): ProgramBuilder {
    if (this.activeProg) {
      return this.activeProg.if(condition, func);
    }
    const subprog = new ProgramBuilder(this.settings, {
      rootProgram: this.rootProgram || this,
      parentProgram: this,
      userHandle: this.userHandle,
      condition,
    });
    this.subPrograms.push(subprog);

    if (func) {
      this.activeProg = subprog;
      try {
        func();
      } finally {
        this.activeProg = undefined;
      }
    }

    if (elseFunc) {
      this.if(condition.not(), elseFunc);
    }

    return subprog;
  }

  ifNot(condition: DataHandle, func?: SubprogScope): ProgramBuilder {
    return this.if(condition.not(), func);
  }

  asUser(accountID: DataHandle, func?: SubprogScope): ProgramBuilder {
    if (this.activeProg) {
      return this.activeProg.asUser(accountID, func);
    }
    if (this.settings.canRunOnClient) {
      throw new Error('asUser can only be run on the server');
    }

    const userHandle = this.addDataFetch('lookupUser', ['account', accountID], undefined, accountID).applyMask(Sketch.ACCOUNT_MASK);
    userHandle.isUsed = true;

    const subprog = new ProgramBuilder(this.settings, {
      rootProgram: this.rootProgram || this,
      parentProgram: this,
      userHandle,
    });
    this.subPrograms.push(subprog);

    if (func) {
      this.activeProg = subprog;
      try {
        func();
      } finally {
        this.activeProg = undefined;
      }
    }
    return subprog;
  }

  asAdmin(func?: SubprogScope): ProgramBuilder {
    if (this.activeProg) {
      return this.activeProg.asAdmin(func);
    }
    if (this.settings.canRunOnClient) {
      throw new Error('asAdmin can only be run on the server');
    }

    const subprog = new ProgramBuilder(this.settings, {
      rootProgram: this.rootProgram || this,
      parentProgram: this,
    });
    this.subPrograms.push(subprog);

    if (func) {
      this.activeProg = subprog;
      try {
        func();
      } finally {
        this.activeProg = undefined;
      }
    }
    return subprog;
  }

  runPermCheck(permFunc: Function, ...args: (string|number|DataHandle)[]) {
    if (this.rootProgram) {
      throw new Error('runPermCheck can only run at the program root');
    }

    if (this.activeProg) {
      this.activeProg.runPermCheck(permFunc, ...args);
      return;
    }
    const subprog = new ProgramBuilder(this.settings, {
      rootProgram: this.rootProgram || this,
      parentProgram: this,
      userHandle: this.userHandle,
    });
    this.subPrograms.push(subprog);

    permFunc(subprog, ...args);

    // update write depth so writes come after permission functions
    this.getWriteDepth(subprog.maxDepth, ...args);

    this.permFuncs.push(permFunc);
  }

  genUUID(name: string, optType?: string): DataHandle {
    if (this.rootProgram) {
      return this.rootProgram.genUUID(name, optType);
    }

    if (!this.uuids[name]) {
      this.uuids[name] = {
        handle: new DataHandle(this.rootProgram || this, `uuids<${name}>`, this.minDepth),
        optType,
      };
    }
    return this.uuids[name].handle;
  }

  clientTime(): DataHandle {
    if (this.rootProgram) {
      return this.rootProgram.clientTime();
    }
    if (!this.clientTimeHandle) {
      this.clientTimeHandle = new DataHandle(this.rootProgram || this, `clientTime`, this.minDepth);
    }
    return this.clientTimeHandle;
  }

  serverTime(): DataHandle {
    if (this.rootProgram) {
      return this.rootProgram.serverTime();
    }
    if (!this.serverTimeHandle) {
      this.serverTimeHandle = new DataHandle(this.rootProgram || this, `serverTime`, this.minDepth);
    }
    return this.serverTimeHandle;
  }

  getData(path: DataPath, mask?: SketchMask): DataHandle {
    if (this.activeProg) {
      return this.activeProg.getData(path, mask);
    }
    const type = this.userHandle ? 'getClientData' : 'getServerData';
    return this.addDataFetch(type, path, mask);
  }

  callSync(func: Function, ...args): DataHandle {
    if (this.activeProg) {
      return this.activeProg.callSync(func, ...args);
    }
    const depth = getMaxHandleDepth(this.rootProgram || this, this.minDepth, args);
    const handle = new DataHandle(this.rootProgram || this, `${func.name}()`, depth + 1);
    const op: ExternalFunc = {
      opType: 'ExternalFunc',
      conditions: [],
      isSync: true,
      user: this.userHandle,
      handle,
      func,
      args,
    };
    this.addOperation(depth, op);
    this.updateMaxDepth(handle.depth);
    return handle;
  }

  callAsync(func: Function, ...args): DataHandle {
    if (this.activeProg) {
      return this.activeProg.callAsync(func, ...args);
    }
    const depth = getMaxHandleDepth(this.rootProgram || this, this.minDepth, args);
    const handle = new DataHandle(this.rootProgram || this, `${func.name}()`, depth + 1);
    const op: ExternalFunc = {
      opType: 'ExternalFunc',
      conditions: [],
      isSync: false,
      user: this.userHandle,
      handle,
      func,
      args,
    };
    this.addOperation(depth, op);
    this.updateMaxDepth(handle.depth);
    return handle;
  }

  createData(path: DataPath, value: DataHandle|Stash) {
    if (this.activeProg) {
      this.activeProg.createData(path, value);
      return;
    }
    const op: DataWrite = {
      opType: 'DataWrite',
      conditions: [],
      writeType: this.userHandle ? 'insertClientData' : 'insertServerData',
      user: this.userHandle,
      path,
      value,
    };
    this.addOperation(this.getWriteDepth(path, value), op);
  }

  initializeData(path: DataPath, initPath = false) {
    if (this.activeProg) {
      this.activeProg.initializeData(path, initPath);
      return;
    }

    // can write initializeData (if initPath is false) using only program commands:
    // this.if(this.getData(path, {}).hasError('not found')).createData(path, {});

    let writeType: WriteOperation;
    if (this.userHandle) {
      writeType = initPath ? 'initializeClientPath' : 'initializeClientData';
    } else {
      writeType = initPath ? 'initializeServerPath' : 'initializeServerData';
    }
    const op: DataWrite = {
      opType: 'DataWrite',
      conditions: [],
      writeType,
      user: this.userHandle,
      path,
    };
    this.addOperation(this.getWriteDepth(path), op);
  }

  updateData(path: DataPath, value: DataHandle|Stash|string|number|null) {
    if (this.activeProg) {
      this.activeProg.updateData(path, value);
      return;
    }
    const op: DataWrite = {
      opType: 'DataWrite',
      conditions: [],
      writeType: this.userHandle ? 'updateClientData' : 'updateServerData',
      user: this.userHandle,
      path,
      value,
    };
    this.addOperation(this.getWriteDepth(path, value), op);
  }

  replaceData(path: DataPath, value: DataHandle|Stash|string|number|null) {
    if (this.activeProg) {
      this.activeProg.replaceData(path, value);
      return;
    }
    const op: DataWrite = {
      opType: 'DataWrite',
      conditions: [],
      writeType: this.userHandle ? 'replaceClientData' : 'replaceServerData',
      user: this.userHandle,
      path,
      value,
    };
    this.addOperation(this.getWriteDepth(path, value), op);
  }

  addMember(path: DataPath, accountID: DataHandle, value: DataHandle|Stash) {
    if (this.activeProg) {
      this.activeProg.addMember(path, accountID, value);
      return;
    }
    if (path.length > 2) {
      throw new Error('Invalid path for addMember call');
    }

    const op: DataWrite = {
      opType: 'DataWrite',
      conditions: [],
      writeType: 'addMember',
      user: this.userHandle,
      path,
      value,
      accountID,
    };
    this.addOperation(this.getWriteDepth(path, accountID, value), op);
  }

  removeMember(path: DataPath, accountID: DataHandle) {
    if (this.activeProg) {
      this.activeProg.removeMember(path, accountID);
      return;
    }
    if (path.length > 2) {
      throw new Error('Invalid path for removeMember call');
    }

    const op: DataWrite = {
      opType: 'DataWrite',
      conditions: [],
      writeType: 'removeMember',
      user: this.userHandle,
      path,
      accountID,
    };
    this.addOperation(this.getWriteDepth(path, accountID), op);
  }

  removeData(path: DataPath) {
    if (this.activeProg) {
      this.activeProg.removeData(path);
      return;
    }
    const op: DataWrite = {
      opType: 'DataWrite',
      conditions: [],
      writeType: this.userHandle ? 'removeClientData' : 'removeServerData',
      user: this.userHandle,
      path,
    };
    this.addOperation(this.getWriteDepth(path), op);
  }

  abortOnError(handle: DataHandle) {
    if (this.activeProg) {
      this.activeProg.abortOnError(handle);
      return;
    }
    const depth = getMaxHandleDepth(this.rootProgram || this, this.minDepth, handle);
    const op: AbortOnError = {
      opType: 'AbortOnError',
      conditions: [],
      user: this.userHandle,
      handle,
    };
    this.addOperation(depth, op);
    this.updateMaxDepth(depth);
  }

  compileProgram() {
    let operations: ProgramOp[] = [];
    for (let i = 0; i <= this.maxDepth; ++i) {
      const ops = this.operations[i];
      if (ops) {
        operations = operations.concat(ops);
      }
    }

    for (const key in this.dataHandles) {
      const handle = this.dataHandles[key];
      if (!handle.isUsed) {
        throw new Error(`SketchProgram compile error: handle <${handle}> is unused`);
      }
    }

    return new SketchProgram({
      permFuncs: this.permFuncs,
      dataHandles: this.dataHandles,
      userHandle: this.userHandle,
      inputs: this.inputs,
      uuids: this.uuids,
      clientTimeHandle: this.clientTimeHandle,
      serverTimeHandle: this.serverTimeHandle,
      outputValue: this.outputValue,
      operations: operations,
    });
  }
}

interface ProgramData {
  permFuncs: Function[];
  dataHandles: StashOf<DataHandle>;
  userHandle: DataHandle|undefined;
  inputs: StashOf<OpInput>;
  uuids: StashOf<{ handle: DataHandle, optType?: string }>;
  clientTimeHandle: DataHandle|undefined;
  serverTimeHandle: DataHandle|undefined;
  outputValue: DataHandle|undefined;
  operations: ProgramOp[];
}

export class SketchProgram {
  private data: ProgramData;

  constructor(data: ProgramData) {
    this.data = data;
  }

  getDebugInfo() {
    const res = {
      inputs: {} as StashOf<Types.Schema>,
      uuids: Object.keys(this.data.uuids),
      ops: this.data.operations.map(opToDebugInfo),
      returnValue: this.data.outputValue ? this.data.outputValue.dbgName : undefined,
    };
    for (const name in this.data.inputs) {
      res.inputs[name] = this.data.inputs[name].schema;
    }
    return res;
  }

  getApiParams(): Util.ApiParams {
    const params: Util.ApiParams = {
      names: [],
      types: {},
    };
    for (const name in this.data.inputs) {
      params.names.push(name);
      params.types[name] = this.data.inputs[name].schema;
    }
    return params;
  }

  getPermFuncs() {
    return this.data.permFuncs;
  }

  getHandleByKey(key: string) {
    const handle = this.data.dataHandles[key];
    if (!handle) {
      throw new Error('No such handle: ' + key);
    }
    return handle;
  }

  async run(ctx: Context, args: Stash) {
    const ectx: ExecutionContext = {
      prog: this,
      ctx,
      dataHandleValues: {},
      dataHandleErrors: {},
      accountsModified: {},
      debug: false,
    };

    // fill user handle
    if (this.data.userHandle) {
      if (!ctx.user.id) {
        throw new Error('no user');
      }
      ectx.dataHandleValues[this.data.userHandle.handleKey] = ctx.user;
    }

    // fill inputs
    for (const name in this.data.inputs) {
      const input = this.data.inputs[name];
      const inputVal = (args.hasOwnProperty(name) && args[name] !== undefined && args[name] !== null) ? args[name] : input.defaultValue;
      if (!Types.validateType(inputVal, input.schema, true)) {
        throw new Error('invalid type for arg "' + name + '"');
      }
      // clone the arg in case the operation modifies it
      ectx.dataHandleValues[input.handle.handleKey] = ObjUtils.clone(inputVal);
    }

    // generate UUIDs
    for (const key in this.data.uuids) {
      const handle = this.data.uuids[key].handle;
      ectx.dataHandleValues[handle.handleKey] = Sketch.clientUUID(ectx.ctx, key, this.data.uuids[key].optType);
    }

    // fill in time handles
    if (this.data.clientTimeHandle) {
      ectx.dataHandleValues[this.data.clientTimeHandle.handleKey] = Sketch.clientTime(ectx.ctx);
    }
    if (this.data.serverTimeHandle) {
      ectx.dataHandleValues[this.data.serverTimeHandle.handleKey] = Sketch.serverTime(ectx.ctx);
    }

    for (const op of this.data.operations) {
      switch (op.opType) {
        case 'DataFetch':
          await runDataFetch(ectx, op);
          break;

        case 'DataWrite':
          await runDataWrite(ectx, op);
          break;

        case 'ExternalFunc':
          await runExternalFunc(ectx, op);
          break;

        case 'AbortOnError':
          await runAbortOnError(ectx, op);
          break;

        default:
          absurd(op);
      }
    }

    let retVal;
    try {
      retVal = evalHandles(ectx, this.data.outputValue);
    } catch (err2) {
      if (ectx.debug) {
        console.log(`evalHandles failed: ${this.data.outputValue && this.data.outputValue.dbgName}`, {err2});
        debugger;
      }
      throw err2;
    }
    if (AccountTokenCache) {
      for (const accountID in ectx.accountsModified) {
        AccountTokenCache.resetAccountCache(accountID);
      }
    }
    return retVal;
  }
}

export function initServer(AccountTokenCacheModule: AccountTokenCacheInterface | undefined) {
  AccountTokenCache = AccountTokenCacheModule;
}

export type ProgramBuildFunc = (prog: ProgramBuilder) => void;

function validateBuildFunc(buildFunc: ProgramBuildFunc) {
  if (process.env.NODE_ENV === 'production') {
    return;
  }

  const funcStr = buildFunc.toString();
  if (funcStr.indexOf(' if (') >= 0) {
    throw new Error(`SketchProgram build function contains a JS "if" statement; you probably meant to use "prog.if()" instead`);
  }
}

export function buildSketchActionProgram(buildFunc: ProgramBuildFunc) {
  validateBuildFunc(buildFunc);
  const builder = new ProgramBuilder({ canRunOnClient: true });
  buildFunc(builder);
  return builder.compileProgram();
}

export function buildUserProgram(buildFunc: ProgramBuildFunc) {
  validateBuildFunc(buildFunc);
  const builder = new ProgramBuilder();
  buildFunc(builder);
  return builder.compileProgram();
}

export function buildAdminProgram(buildFunc: ProgramBuildFunc) {
  validateBuildFunc(buildFunc);
  const builder = new ProgramBuilder({ runAsAdmin: true });
  buildFunc(builder);
  return builder.compileProgram();
}
