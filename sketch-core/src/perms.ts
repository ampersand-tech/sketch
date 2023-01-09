/**
* Copyright 2017-present Ampersand Technologies, Inc.
*/

import * as Constants from 'overlib/shared/constants';
import { ACCESS_LEVEL } from 'overlib/shared/constants';
import * as Jobs from 'overlib/shared/jobs';
import { ProgramBuilder } from 'overlib/shared/sketchProgram';
import * as Util from 'overlib/shared/util';

type Mask = number | '*' | { [k: string]: Mask };
export type HideArgsOption = { [k: string]: Mask };

export type HideArgs = {
  idx: number;
  hideMask: Mask;
}[];

let gConfig: Stash|undefined;
let AccountTwoFactor: Stash|undefined;

export function initServer(config: Stash, AccountTwoFactorModule: Stash) {
  gConfig = config;
  AccountTwoFactor = AccountTwoFactorModule;
}

function fillInArgNames(func: PermFunc) {
  const argNames = Util.getFunctionParamNames(func);
  const ctxParam = argNames.shift();
  if (ctxParam !== 'ctx' && ctxParam !== 'ctxIgnored' && ctxParam !== '_ctx') {
    throw new Error('perm function "' + func.name + '" missing ctx param');
  }
  if (argNames.pop() !== 'cb') {
    throw new Error('perm function "' + func.name + '" missing cb param');
  }
  func._argNames = argNames;
}

export interface PermFunc {
  (ctx: Context, ...args: any[]): void;
  _argNames?: string[];
}

export function checkPermFuncs(params: Util.ApiParams, optionName, optionVal: PermFunc[], onApiError): PermFunc[] {
  if (!Array.isArray(optionVal)) {
    onApiError('specifies ' + optionName + ' that should be an array');
    return [];
  }

  for (let i = 0; i < optionVal.length; ++i) {
    const permFunc = optionVal[i];
    if (!(permFunc instanceof Function)) {
      onApiError('specifies ' + optionName + ' arg[' + i + '] that is not a function');
      continue;
    }

    if (!permFunc._argNames) {
      fillInArgNames(permFunc);
    }

    const argNames = permFunc._argNames!;

    for (let j = 0; j < argNames.length; ++j) {
      const argName = argNames[j];
      const argIdx = params.names.indexOf(argName);
      if (argIdx < 0) {
        onApiError(
          'specifies ' + optionName + ' function "' + permFunc.name + '" that wants param "' + argName + '" that the action does not provide',
        );
      }
    }
  }

  return optionVal;
}

export function checkHideArgs(params: Util.ApiParams, optionName: string, optionVal: HideArgsOption | undefined, onApiError): HideArgs {
  if (!optionVal) {
    return [];
  }

  if (!Util.isObject(optionVal)) {
    onApiError('specifies ' + optionName + ' that should be an object');
    return [];
  }

  const hideArgs: HideArgs = [];
  for (const argName in optionVal) {
    const argIdx = params.names.indexOf(argName);
    if (argIdx < 0) {
      onApiError('specifies ' + optionName + ' arg "' + argName + '" that is not in the parameter list');
    }
    hideArgs.push({
      idx: argIdx,
      hideMask: optionVal[argName],
    });
  }
  return hideArgs;
}

export function hideFields(params, where: { [k: string]: Mask }, cloneIfChanging = false) {
  for (const id in where) {
    const mask = where[id];
    if (typeof mask === 'object') {
      if (cloneIfChanging) {
        params[id] = Util.shallowClone(params[id]);
      }
      hideFields(params[id], mask, cloneIfChanging);
    } else {
      params[id] = null;
    }
  }
}

export function genDbgArgs(args: any[], hideArgs: HideArgs | undefined | null): any[] {
  const dbgArgs: any[] = args.slice(0);
  dbgArgs[0] = dbgArgs[0].userDebugKey;
  if (hideArgs) {
    for (let i = 0; i < hideArgs.length; ++i) {
      const hideArgIdx = hideArgs[i].idx + 1; // +1 because of prepended sctx arg
      const mask = hideArgs[i].hideMask;
      if (typeof mask === 'object') {
        dbgArgs[hideArgIdx] = Util.clone(dbgArgs[hideArgIdx]);
        hideFields(dbgArgs[hideArgIdx], mask);
      } else {
        dbgArgs[hideArgIdx] = null;
      }
    }
  }
  return dbgArgs;
}

export function genDbgParams<P>(params: P, hideArgs: HideArgsOption | undefined | null) {
  if (!hideArgs) {
    return params;
  }
  params = Util.shallowClone(params);
  hideFields(params, hideArgs, true);
  return params;
}

export function applyActionArgsToFunction(ctx: Context, argMap: Stash, func: ContextFunction, argNames: string[], cb: ErrDataCB<any>) {
  const funcArgs: any[] = [ctx];
  for (let i = 0; i < argNames.length; ++i) {
    funcArgs.push(argMap[argNames[i]]);
  }
  funcArgs.push(cb);
  func.apply(undefined, funcArgs);
}

export function runPermChecks(ctx: Context, permFuncs: ContextFunction[], argMap: Stash, cb: ErrDataCB<any>) {
  const jobs = new Jobs.Queue();
  for (let i = 0; i < permFuncs.length; ++i) {
    const permFunc = permFuncs[i];
    if (!(permFunc as any)._argNames) {
      fillInArgNames(permFunc);
    }
    jobs.add(applyActionArgsToFunction, ctx, argMap, permFunc, (permFunc as any)._argNames);
  }
  jobs.drain(cb);
}


///////////////////////////////////////
// PERM FUNCTIONS:

export function progAdminOnly(prog: ProgramBuilder) {
  prog.if(prog.user.getProperty('access').notEqualTo(ACCESS_LEVEL.ADMIN)).returnError('need to be admin');
}

export function adminOnly(ctx: Context, cb: ErrDataCB<any>) {
  if (ctx.user.access !== ACCESS_LEVEL.ADMIN) {
    cb('need to be admin');
  } else {
    cb();
  }
}

export function progIsWriter(prog: ProgramBuilder) {
  prog.ifNot(prog.userID).returnError('need to be logged in to do this');
  prog.if(prog.user.getProperty('userType').notEqualTo(Constants.USER_TYPE.WRITER)).returnError('need to be a writer to do this');
  prog.if(prog.user.getProperty('access').equalTo(ACCESS_LEVEL.DELETED)).returnError('need to be a writer to do this');

  const suspension = prog.user.getProperty('suspension');
  prog.if(suspension.and(suspension.notEqualTo(Constants.SUSPENSION_TYPE.NONE))).returnError('account currently suspended');
}

export function isWriter(ctx: Context, cb: ErrDataCB<any>) {
  if (!ctx.user.id || ctx.user.userType !== Constants.USER_TYPE.WRITER || ctx.user.access === Constants.ACCESS_LEVEL.DELETED) {
    cb('need to be a writer to do this');
  } else if (ctx.user.suspension && ctx.user.suspension !== Constants.SUSPENSION_TYPE.NONE) {
    cb('account currently suspended for: ' + ctx.user.suspension);
  } else {
    cb();
  }
}

export function loggedIn(ctx: Context, cb: ErrDataCB<any>) {
  if (!ctx.user.id || ctx.user.access === Constants.ACCESS_LEVEL.DELETED) {
    cb('need to be logged in to do this');
  } else if (ctx.user.suspension && ctx.user.suspension !== Constants.SUSPENSION_TYPE.NONE) {
    cb('account currently suspended for: ' + ctx.user.suspension);
  } else {
    cb();
  }
}


///////////////////////////////////////
// SERVER ONLY:

export function test(_ctx: ServerContext, cb: ErrDataCB<any>) {
  if (!gConfig) {
    cb('permissions check only valid on the server');
  } else if (!gConfig.testSuite) {
    cb('need to be testclient');
  } else {
    cb();
  }
}

export function testAdmin(ctx: ServerContext, cb) {
  if (!gConfig) {
    cb('permissions check only valid on the server');
  } else if (!gConfig.testSuite) {
    cb('need to be testclient');
  } else {
    adminOnly(ctx, cb);
  }
}

export function twoFactor(ctx: ServerContext, cb: ErrDataCB<any>) {
  if (!gConfig) {
    cb('permissions check only valid on the server');
    return;
  }
  if (gConfig.env === 'development' && ctx.totpCode === 'test2f') {
    // allow developers and test clients to get through with special code
    return cb();
  }

  if (!AccountTwoFactor) {
    cb('permissions check only valid on the server');
    return;
  }
  AccountTwoFactor.validateTotpCode(ctx, ctx.user.id, ctx.totpCode, true, function(err2) {
    if (err2) { return cb('PromptTwoFactor'); }
    cb();
  });
}

export function admin2Fac(ctx: ServerContext, cb: ErrDataCB<any>) {
  adminOnly(ctx, function(err1) {
    if (err1) { return cb(err1); }
    twoFactor(ctx, cb);
  });
}
