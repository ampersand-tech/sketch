/**
* Copyright 2015-present Ampersand Technologies, Inc.
*/

import { ProgramBuilder, SketchProgram, buildSketchActionProgram } from './SketchProgram';
import * as Types from './types';

import * as ErrorUtils from 'amper-utils/dist2017/errorUtils';
import * as ObjUtils from 'amper-utils/dist2017/objUtils';
import { wrap, wrapMember } from 'amper-utils/dist2017/promiseUtils';
import * as StringUtils from 'amper-utils/dist2017/stringUtils';
import { Stash, StashOf } from 'amper-utils/dist2017/types';
//import * as Perms from 'overlib/shared/perms';

import {
  ActionPayload,
  SketchContext,
  SketchServerContext,
} from './SketchTypes';

let DataStore;
let DBUtil;
let Feed;


const MAX_DEADLOCK_RETRIES = 5;

const gTransactAction = true;

let gIsClient = false;
let gIsServer = false;

let gPreActionHook: undefined | (() => any);
let gPostActionHook: undefined | (() => any);

interface ActionOptions {
  perms?: ContextFunction[];
  progPerms?: Function[];
  paramTypes: Util.ParamTypes;
  supersedeBy?: string[];
  accumulateBy?: string[];
  metric?: {
    set: Metrics.MetricSet;
    name: string;
  };
  _deprecated?: boolean;
}

interface ActionProgramOptions {
  supersedeBy?: string[];
  accumulateBy?: string[];
  _deprecated?: boolean;
}

interface ActionDef {
  name: string;
  options: ActionOptions;
  func?: ContextFunction;
  prog?: SketchProgram;
  argNames: string[];
  argTypes: Util.ParamTypes;
  serverFunc?: ServerContextFunction;
  asyncServerFunc?: (ctx: ServerContext, params: Stash) => Promise<void>;
  serverProg?: SketchProgram;
  serverArgNames: string[];
  permFuncs: ContextFunction[];
  isAdminOnly: boolean;
  isDeprecated: boolean;
}

type ProgBuildFunc = (prog: ProgramBuilder) => void;

const gActions: StashOf<ActionDef> = {};


export function initClient(dataStoreModule, dbUtilModule) {
  gIsClient = true;
  DataStore = dataStoreModule;
  DBUtil = dbUtilModule;
}

export function initServer(feedModule) {
  gIsServer = true;
  Feed = feedModule;
}

function checkSupersedeBy(params, optionName: string, optionVal: string[], onApiError) {
  if (!Array.isArray(optionVal)) {
    return onApiError('specifies ' + optionName + ' that should be an array');
  }

  const validArgs: string[] = [];

  for (let i = 0; i < optionVal.length; ++i) {
    const argName = optionVal[i];
    if (params.names.indexOf(argName) < 0) {
      onApiError('specifies ' + optionName + ' arg "' + argName + '" that is not in the parameter list');
      continue;
    }
    const argType = params.types[argName];
    if (!argType) {
      onApiError('specifies ' + optionName + ' arg "' + argName + '" that has no type information');
      continue;
    }
    if (!Types.isType(params.types[argName])) {
      onApiError('specifies ' + optionName + ' arg "' + argName + '" that is not a type');
      continue;
    }
    const typeStr = argType.toString();
    if (StringUtils.startsWith(typeStr, 'OBJECT') || StringUtils.startsWith(typeStr, 'ARRAY') || StringUtils.startsWith(typeStr, 'STRING_ARRAY')) {
      onApiError('specifies ' + optionName + ' arg "' + argName + '" that is a disallowed type');
      continue;
    }
    validArgs.push(argName);
  }
  return validArgs;
}

function checkParamTypes(_params: Util.ApiParams, optionName: string, optionVal, onApiError) {
  if (!ObjUtils.isObject(optionVal)) {
    return onApiError('specifies ' + optionName + ' that should be an object');
  }
  return optionVal;
}

function noCheck(_params: Util.ApiParams, _optionName: string, optionVal: any, _onApiError) {
  return optionVal;
}

const ACTION_OPTIONS = {
  perms: Perms.checkPermFuncs,
  paramTypes: checkParamTypes,
  supersedeBy: checkSupersedeBy,
  accumulateBy: checkSupersedeBy,
  _deprecated: noCheck,
  metric: noCheck,
};

export function defineAction(actionName: string, func: ContextFunction, options: ActionOptions): undefined | ActionDef {
  function onApiError(err): undefined {
    throw new Error('Sketch: action "' + actionName + '" ' + err);
  }

  if (gActions[actionName]) {
    return onApiError('already defined');
  }

  const params = Util.getApiParams(func, options.paramTypes, onApiError);
  if (!params) {
    return onApiError('failed to parse function');
  }

  // validate options
  for (const optionName in options) {
    if (!ACTION_OPTIONS.hasOwnProperty(optionName)) {
      onApiError('specifies unknown option "' + optionName + '"');
      continue;
    }
    options[optionName] = ACTION_OPTIONS[optionName](params, optionName, options[optionName], onApiError);
  }

  if (options.supersedeBy && options.accumulateBy) {
    onApiError('specifies both "supersedeBy" and "accumulateBy" options, which are incompatible');
  }

  // register action
  const sketchAction: ActionDef = {
    name: actionName,
    options: options,
    func: func,
    prog: undefined,
    argNames: params.names,
    argTypes: params.types,
    serverFunc: undefined,
    asyncServerFunc: undefined,
    serverProg: undefined,
    serverArgNames: [],
    permFuncs: options.perms || [],
    isAdminOnly: options.perms ? options.perms.indexOf(Perms.adminOnly) >= 0 : false,
    isDeprecated: Boolean(options._deprecated),
  };

  gActions[actionName] = sketchAction;
  return sketchAction;
}

export function defineActionProgram(actionName: string, progBuilder: ProgBuildFunc);
export function defineActionProgram(actionName: string, options: ActionProgramOptions, progBuilder: ProgBuildFunc);
export function defineActionProgram(actionName: string, ...args) {
  const options: ActionProgramOptions = args.length === 2 ? args[0] : {};
  const progBuilder: ProgBuildFunc = args.length === 2 ? args[1] : args[0];

  function onApiError(err) {
    throw new Error('Sketch: action "' + actionName + '" ' + err);
  }

  if (gActions[actionName]) {
    return onApiError('already defined');
  }

  const prog = buildSketchActionProgram(progBuilder);
  const params = prog.getApiParams();

  // validate options
  for (const optionName in options) {
    if (!ACTION_OPTIONS.hasOwnProperty(optionName)) {
      onApiError('specifies unknown option "' + optionName + '"');
      continue;
    }
    options[optionName] = ACTION_OPTIONS[optionName](params, optionName, options[optionName], onApiError);
  }

  if (options.supersedeBy && options.accumulateBy) {
    onApiError('specifies both "supersedeBy" and "accumulateBy" options, which are incompatible');
  }

  const fullOptions: ActionOptions = {
    perms: [],
    progPerms: prog.getPermFuncs(),
    paramTypes: params.types,
    supersedeBy: options.supersedeBy,
    accumulateBy: options.accumulateBy,
    _deprecated: options._deprecated,
  };

  // register action
  const sketchAction: ActionDef = {
    name: actionName,
    options: fullOptions,
    func: undefined,
    prog,
    argNames: params.names,
    argTypes: params.types,
    serverFunc: undefined,
    asyncServerFunc: undefined,
    serverProg: undefined,
    serverArgNames: [],
    permFuncs: [],
    isAdminOnly: fullOptions.progPerms ? fullOptions.progPerms.indexOf(Perms.progAdminOnly) >= 0 : false,
    isDeprecated: Boolean(fullOptions._deprecated),
  };

  gActions[actionName] = sketchAction;
  return sketchAction;
}

export function registerServerAction(actionName: string, func: ServerContextFunction) {
  const sketchAction = gActions[actionName];
  if (!sketchAction) {
    throw new Error('Sketch: action "' + actionName + '" does not exist');
  }

  if (sketchAction.serverFunc || sketchAction.asyncServerFunc || sketchAction.serverProg) {
    throw new Error('Sketch: action "' + actionName + '" already has a server action');
  }

  // gather and validate args
  const argNames = Util.getFunctionParamNames(func);
  if (argNames.shift() !== 'ctx') {
    throw new Error('Sketch: server action "' + actionName + '" missing ctx param');
  }
  if (argNames.pop() !== 'cb') {
    throw new Error('Sketch: server action "' + actionName + '" missing cb param');
  }

  for (let i = 0; i < argNames.length; ++i) {
    if (sketchAction.argNames.indexOf(argNames[i]) < 0) {
      throw new Error('Sketch: server action "' + actionName + '" has argument "' + argNames[i] + '" that is not in the shared action');
    }
  }

  sketchAction.serverFunc = func;
  sketchAction.serverArgNames = argNames;
}

export function registerAsyncServerAction(actionName: string, func: (ctx: ServerContext, params: Stash) => Promise<void>) {
  const sketchAction = gActions[actionName];
  if (!sketchAction) {
    throw new Error('Sketch: action "' + actionName + '" does not exist');
  }

  if (sketchAction.serverFunc || sketchAction.asyncServerFunc || sketchAction.serverProg) {
    throw new Error('Sketch: action "' + actionName + '" already has a server action');
  }

  sketchAction.asyncServerFunc = func;
}

function callActionFunc(ctx: Context, sketchAction: ActionDef, sketchActionData: ActionPayload, cb: SketchCB) {
  const args: any[] = [ctx];
  for (let i = 0; i < sketchAction.argNames.length; ++i) {
    const argName = sketchAction.argNames[i];
    const argType = sketchAction.argTypes[argName];
    const argVal = sketchActionData.argMap.hasOwnProperty(argName) ? sketchActionData.argMap[argName] : null;
    if (!Types.validateType(argVal, argType)) {
      cb('invalid type for arg "' + argName + '"');
      return;
    }
    // clone the argument in case the action modifies it
    args.push(Util.clone(argVal));
  }
  args.push(cb);

  sketchAction.func!.apply(null, args);
}

async function runClientAction(errPrefix: string, sketchAction: ActionDef, sketchActionData: ActionPayload, noWatchTrigger: boolean, isReplay: boolean) {
  const res = {
    hadError: false,
    result: null,
  };

  const ctx: SketchContext = {
    user: DataStore.getData(null, ['account'], Sketch.ACCOUNT_MASK),
    sketchActionData: sketchActionData,
    sketchServerActionData: {},
    sketchNoWatchTrigger: !!noWatchTrigger,
    sketchIsReplay: !!isReplay,
    dims: {},
  };

  if (!ctx.user.id) {
    res.hadError = true;
    console.error(errPrefix + 'called with no account ID');
    return res;
  }

  console.debug(errPrefix + 'running', sketchActionData);

  try {
    if (sketchAction.prog) {
      res.result = await sketchAction.prog.run(ctx, sketchActionData.argMap);
    } else {
      await Perms.runPermChecks(ctx, sketchAction.permFuncs, sketchActionData.argMap);
      res.result = await callActionFunc(ctx, sketchAction, sketchActionData);
    }
  } catch (err) {
    res.hadError = true;
    console.error(errPrefix + 'action returned an error', {err: err, args: sketchActionData.argMap});
  }

  ctx.sketchServerActionData = undefined;
  ctx.sketchActionData = undefined;

  return res;
}

function validateRunAction(actionName: string) {
  const errPrefix = 'Sketch.runAction(' + actionName + '): ';
  if (!gIsClient) {
    console.error(errPrefix + 'can only be called on the client');
    return { errPrefix, sketchAction: undefined };
  }

  const sketchAction = gActions[actionName];
  if (!sketchAction) {
    console.error(errPrefix + 'action does not exist');
    return { errPrefix, sketchAction: undefined };
  }

  if (sketchAction.isDeprecated) {
    console.warn('runAction.deprecatedCall', {deprecatedAction: actionName});
  }

  return { errPrefix, sketchAction };
}

async function runAndDispatchClientAction(actionName: string, args: any[], runSync: boolean) {
  const {errPrefix, sketchAction} = validateRunAction(actionName);
  if (!sketchAction) {
    return null;
  }

  if (args.length !== sketchAction.argNames.length) {
    console.error(errPrefix + 'incorrect number of arguments for action', {args: args});
    return null;
  }

  const argMap = {};
  for (let i = 0; i < args.length; ++i) {
    const argName = sketchAction.argNames[i];
    // clone here in case caller decides to modify after this call
    argMap[argName] = ObjUtils.clone(args[i]);
  }

  return await runAndDispatchClientActionWithArgs(actionName, argMap, runSync);
}

async function runAndDispatchClientActionWithArgs(actionName: string, argMap: Stash, runSync: boolean) {
  const {errPrefix, sketchAction} = validateRunAction(actionName);
  if (!sketchAction) {
    return null;
  }

  const sketchActionData: ActionPayload = {
    argMap: argMap,
    clientKey: DBUtil.uuid('ck'),
    clientUUIDs: {},
    clientTime: undefined,
    serverTime: undefined,
  };

  gPreActionHook && gPreActionHook();

  const res = await runClientAction(errPrefix, sketchAction, sketchActionData, false, false);
  if (res.hadError) {
    DBUtil.replayChangesOnSketchError();
  } else {
    // dispatch action to server
    DBUtil.dispatchAction(sketchAction, sketchActionData, runSync);
  }

  gPostActionHook && gPostActionHook();

  return res.result;
}

export async function runAction(actionName: string, ...args) {
  return await runAndDispatchClientAction(actionName, args, true);
}

export async function runActionNoSync(actionName: string, ...args) {
  return await runAndDispatchClientAction(actionName, args, false);
}

export async function runActionWithArgs(actionName: string, argMap: Stash) {
  return await runAndDispatchClientActionWithArgs(actionName, argMap, true);
}

export async function replayAction(actionName: string, sketchActionData: ActionPayload, noWatchTrigger: boolean) {
  const errPrefix = 'Sketch.replayAction(' + actionName + '): ';
  if (!gIsClient) {
    console.error(errPrefix + 'can only be called on the client');
    return null;
  }

  const sketchAction = gActions[actionName];
  if (!sketchAction) {
    console.error(errPrefix + 'action does not exist');
    return null;
  }

  if (sketchAction.isDeprecated) {
    console.warn('runAction.deprecatedCall', {deprecatedAction: actionName});
  }

  gPreActionHook && gPreActionHook();

  await runClientAction(errPrefix, sketchAction, sketchActionData, noWatchTrigger, true);

  gPostActionHook && gPostActionHook();
}

export async function mergeAndWriteFeed(ctx: ServerContext) {
  if (!Feed) {
    return;
  }

  const sctx = ctx as SketchServerContext;
  const feedToWrite = Feed.mergeFeedEntries(sctx.sketchFeedToWrite);
  sctx.sketchFeedToWrite = undefined;

  for (let i = 0; i < feedToWrite.length; ++i) {
    const feedData = feedToWrite[i];
    await wrap(Feed.addMulti, sctx, feedData.accountIDs, feedData.keys, feedData.fields, feedData.clientKey);
  }
}

function tagsWithClientInfo(ctx: SketchServerContext, tags: Stash) {
  const clientInfo = ctx ? ctx.clientInfo : null;
  const fullTags = clientInfo ? ObjUtils.clone(clientInfo) : {};
  ObjUtils.copyFields(tags, fullTags);
  return fullTags;
}

export async function dispatchAction(ctx: ServerContext, actionName: string, sketchActionData: ActionPayload, retryCount = 0) {
  const sctx = ctx as SketchServerContext;
  if (!gIsServer) {
    throw new Error('Sketch: dispatchAction can only be called on the server');
  }
  if (!sctx.user || !sctx.user.id) {
    Metrics.recordInSet(ctx, Metrics.SET.INTERNAL.SKETCH, 'dispatch.badData', tagsWithClientInfo(sctx, { reason: 'notLoggedIn' }));
    throw new Error('badData');
  }
  if (!sketchActionData || !sketchActionData.clientKey) {
    Metrics.recordInSet(ctx, Metrics.SET.INTERNAL.SKETCH, 'dispatch.badData', tagsWithClientInfo(sctx, { reason: 'missingSketchData' }));
    throw new Error('badData');
  }

  const clientKeyParsed = Util.parseUUID(sketchActionData.clientKey);
  if (!clientKeyParsed || clientKeyParsed.accountID !== sctx.user.id || clientKeyParsed.optType !== 'ck') {
    Metrics.recordInSet(ctx, Metrics.SET.INTERNAL.SKETCH, 'dispatch.badData', tagsWithClientInfo(sctx, { reason: 'badClientKey' }));
    throw new Error('badData');
  }

  for (const name in sketchActionData.clientUUIDs) {
    const clientUUIDParsed = Util.parseUUID(sketchActionData.clientUUIDs[name]);
    if (!clientUUIDParsed || clientUUIDParsed.accountID !== sctx.user.id) {
      Metrics.recordInSet(ctx, Metrics.SET.INTERNAL.SKETCH, 'dispatch.badData', tagsWithClientInfo(sctx, { reason: 'badClientUUID' }));
      throw new Error('badData');
    }
  }

  const sketchAction = gActions[actionName];
  if (!sketchAction) {
    Log.warn(ctx, '@conor', 'sketch.unknownAction', actionName);
    Metrics.recordInSet(ctx, Metrics.SET.INTERNAL.SKETCH, 'dispatch.badData', tagsWithClientInfo(sctx, { reason: 'unknownAction' }));
    throw new Error('badData');
  }


  const args: any[] = [sctx];
  for (const argName of sketchAction.argNames) {
    const argType = sketchAction.argTypes[argName];
    const argVal = sketchActionData.argMap.hasOwnProperty(argName) ? sketchActionData.argMap[argName] : null;
    if (!Types.validateType(argVal, argType)) {
      Log.warn(ctx, '@conor', 'sketch.invalidArgType', { name: actionName, argName: argName, argValue: Util.safeStringify(argVal, 1000) });
      Metrics.recordInSet(ctx, Metrics.SET.INTERNAL.SKETCH, 'dispatch.badData', tagsWithClientInfo(sctx, { reason: 'invalidArgType' }));
      throw new Error('badData');
    }
    args.push(argVal);
  }

  // debug log
  const dbgArgs = Perms.genDbgArgs(args, null);
  Log.cmd(ctx, actionName, Log.truncateArgs(dbgArgs, sctx.queryStr.length));

  // always use the server's timestamp for serverTime
  delete sketchActionData.serverTime;

  if (gTransactAction) {
    await wrap(Sketch.startTransaction, sctx, actionName);
  } else {
    sctx.sketchFeedToWrite = [];
  }

  let err: ErrorType;
  try {
    sctx.sketchActionData = sketchActionData;
    sctx.sketchServerAction = false;
    sctx.sketchServerActionName = actionName;
    sctx.sketchServerActionData = {};

    if (gTransactAction && sctx.sql) {
      sctx.sql.errorsToNotLog = {};

      // don't log deadlocks in the sql layer, we handle them in endActionTransaction
      sctx.sql.errorsToNotLog['ER_LOCK_DEADLOCK'] = 1;

      if (retryCount) {
        // don't log duplicate entry in the sql layer, we ignore the error on client retry
        // (because a previous transaction may have succeeded without us knowing)
        sctx.sql.errorsToNotLog['ER_DUP_ENTRY'] = 1;
      }
    }

    if (sketchAction.prog) {
      await wrapMember(sketchAction.prog, sketchAction.prog.run, ctx, sketchActionData.argMap);
    } else {
      // permissions checks
      await wrap(Perms.runPermChecks, ctx, sketchAction.permFuncs, sketchActionData.argMap);

      // run action function
      await wrap((next) => {
        sketchAction.func!.apply(null, args.concat(next));
      });
    }

    if (sketchAction.serverFunc) {
      sctx.sketchServerAction = true;

      await wrap(
        Perms.applyActionArgsToFunction,
        sctx as Context,
        sketchActionData.argMap,
        sketchAction.serverFunc as ContextFunction,
        sketchAction.serverArgNames,
      );
    } else if (sketchAction.asyncServerFunc) {
      sctx.sketchServerAction = true;

      await sketchAction.asyncServerFunc(sctx, sketchActionData.argMap);
    }

    if (!gTransactAction) {
      // feed write will happen as part of endTransaction if using transactions
      await mergeAndWriteFeed(sctx);
    }
  } catch (err_) {
    err = err_;
  }

  if (!err && sketchAction.options.metric) {
    const customDims = sctx.sketchServerActionData ? sctx.sketchServerActionData.metricDims : undefined;
    const dims = ObjUtils.shallowCloneAndCopy(sketchActionData.argMap, customDims || {});
    Metrics.recordInSet(sctx, sketchAction.options.metric.set, sketchAction.options.metric.name, dims);
  }

  sctx.sketchActionData = undefined;
  sctx.sketchServerAction = false;
  sctx.sketchServerActionName = undefined;
  sctx.sketchServerActionData = undefined;

  if (sctx.sql) {
    sctx.sql.errorsToNotLog = undefined;
  }

  if (!gTransactAction) {
    sctx.sketchFeedToWrite = undefined;
    if (err) {
      throw err;
    }
    return;
  }

  try {
    await wrap(Sketch.endTransaction, ctx, actionName, err, null);
  } catch (err2) {
    if (ErrorUtils.errorToCode(err2) === 'ER_LOCK_DEADLOCK') {
      Log.info(ctx, 'deadlock detected, retrying action ' + actionName, err2);
      if (retryCount < MAX_DEADLOCK_RETRIES) {
        // rerun action/transaction on deadlock
        return await dispatchAction(ctx, actionName, sketchActionData, retryCount + 1);
      }
    }

    if (ErrorUtils.errorToCode(err2) === 'ER_DUP_ENTRY' && retryCount) {
      // treat ER_DUP_ENTRY as success if this is a retry
      // must do this after endTransaction so the error causes a transaction rollback
      return;
    }

    throw err2;
  }
}


export function clientUUID(ctx: Context, name: string, optType?: string): string {
  const sctx = ctx as SketchContext;
  if (!sctx.sketchActionData) {
    throw new Error('Sketch: clientUUID can only be called from within an action');
  }
  if (!name) {
    throw new Error('Sketch: clientUUID requires a unique name');
  }

  if (gIsClient && !sctx.sketchIsReplay) {
    if (sctx.sketchActionData.clientUUIDs[name]) {
      throw new Error('Sketch: clientUUID already called with name "' + name + '"');
    }
    // generate and send the uuid down to the server
    sctx.sketchActionData.clientUUIDs[name] = DBUtil.uuid(optType);
    return sctx.sketchActionData.clientUUIDs[name];
  }

  // use value sent down from the client
  if (sctx.sketchActionData.clientUUIDs[name]) {
    return sctx.sketchActionData.clientUUIDs[name];
  }

  throw new Error('Sketch: missing clientUUID value');
}

export function clientTime(ctx: Context): number {
  const sctx = ctx as SketchContext;
  if (!sctx.sketchActionData) {
    throw new Error('Sketch: clientTime can only be called from within an action');
  }

  if (gIsClient && !sctx.sketchIsReplay) {
    // generate and send the time down to the server
    if (!sctx.sketchActionData.clientTime) {
      sctx.sketchActionData.clientTime = Date.now();
    }
    return sctx.sketchActionData.clientTime;
  }

  // use value sent down from the client, as long as it is in the past
  const nowTime = Date.now();
  if (sctx.sketchActionData.clientTime) {
    return Util.min(sctx.sketchActionData.clientTime, nowTime);
  }
  return nowTime;
}

export function serverTime(ctx: Context): number {
  const sctx = ctx as SketchContext;
  if (!sctx.sketchActionData) {
    throw new Error('Sketch: serverTime can only be called from within an action');
  }

  // serverTime gets deleted by dispatchAction so this will generate a new timestamp on the server
  if (!sctx.sketchActionData.serverTime) {
    sctx.sketchActionData.serverTime = Date.now();
  }
  return sctx.sketchActionData.serverTime;
}

export type ActionSchema = StashOf<{
  names: string[];
  types: Util.ParamTypes;
}>;
export function getActionSchema(ignoreAdminOnly: boolean): ActionSchema {
  const schema: ActionSchema = {};

  for (const name in gActions) {
    const action = gActions[name];
    if (ignoreAdminOnly && action.isAdminOnly) {
      continue;
    }
    schema[name] = {
      names: action.argNames,
      types: action.argTypes,
    };
  }

  return schema;
}

export const test = {
  setActionHooks(preActionHook: () => any, postActionHook: () => any) {
    gPreActionHook = preActionHook;
    gPostActionHook = postActionHook;
  },
};
