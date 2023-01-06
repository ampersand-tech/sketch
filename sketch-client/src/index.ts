/**
* Copyright 2018-present Ampersand Technologies, Inc.
*/

import { AccountID, BackingContext, SketchBackendInterface, UserInfo } from '../lib/index';

import * as ObjUtils from 'amper-utils/dist/objUtils';

let DataStore;

const ACCOUNT_MASK = ObjUtils.objectMakeImmutable({
  id: 1,
  access: 1,
  userType: 1,
  tableSubs: { _ids: 1 },
});

let gClientUser: UserInfo|undefined;

export class SketchDataStoreBackend extends SketchBackendInterface {
  async init() {
  }

  async getUser(accountID: AccountID): Promise<UserInfo> {
    if (!gClientUser && DataStore.hasDataStore('account')) {
      gClientUser = DataStore.getData(null, ['account'], ACCOUNT_MASK);
      if (!gClientUser.id) {
        gClientUser = undefined;
      }
    }
    if (gClientUser && gClientUser.id !== accountID) {
      throw new Error(`cannot access account "${accountID}" on the client`);
    }
    return gClientUser || {
      id: accountID,
      access: '',
      userType: '',
      tableSubs: {},
    };
  }

  async startTransaction(_ctx: BackingContext, _name: string) {
  }

  async commitTransaction(_ctx: BackingContext) {
    // dispatch action to server
    //DBUtil.dispatchAction(sketchAction, sketchActionData, runSync);
  }

  async rollbackTransaction(_err: any, _ctx: BackingContext) {
    //DBUtil.replayChangesOnSketchError();
  }

  async mergeAndWriteFeed(_ctx: BackingContext, _feedEntries: Stash[]) {
  }
}
