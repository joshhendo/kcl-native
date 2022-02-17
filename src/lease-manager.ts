import {claimLease, listLeases} from "./dynamodb/dynamodb-lease-coordinater";
import {Lease} from "./dynamodb/dynamodb-repository";
import * as dateFns from 'date-fns';
import {Properties} from "./properties-manager";
import {startWorker} from "./kinesis-worker";
import AWS = require("aws-sdk");
import {ShardManager} from "./shard-manager";
import {RecordProcessor} from "aws-kcl";
import {Kinesis} from "aws-sdk";


let MAX_LEASES_OWNED = 1;

interface LeaseListItem {
  lease: Lease;
  last_updated: Date;
}

let allLeases: LeaseListItem[] = [];
let ownedLeases: string[] = [];
const workers: { [key: string]: any } = {};

let client: Kinesis;
let shardManager: ShardManager;

// https://github.com/singular-labs/amazon-kinesis-client/blob/acc61ea41dcc83b90e9d752eb555302503a30891/amazon-kinesis-client/src/main/java/software/amazon/kinesis/leases/dynamodb/DynamoDBLeaseTaker.java#L251
async function updateAllLeases() {
  const currentLeases = await listLeases();

  for (const currentLease of currentLeases) {
    const existingLease = allLeases.find(x => x.lease.leaseKey === currentLease.leaseKey);

    if (existingLease) {
      if (existingLease.lease.leaseCounter !== currentLease.leaseCounter) {
        existingLease.lease = currentLease;
        existingLease.last_updated = new Date();
      }
    } else {
      allLeases.push({
        lease: currentLease,
        last_updated: new Date(),
      });
    }
  }
}

async function getExpiredLeases() {
  return allLeases
    .filter(x => x.lease.checkpoint !== 'SHARD_END')
    .filter(x => dateFns.differenceInMilliseconds(new Date(), x.last_updated) >= Properties.failoverTimeMillis);
}


function createKillFunction(shardKey: string) {
  let killed = false;
  return () => {
    if (killed) {
      return;
    }
    killed = true;
    delete workers[shardKey];
  }
}

function createWorker(shardKey: string, checkpoint: string | null, rp: RecordProcessor) {
  workers[shardKey] = startWorker(shardKey, checkpoint, createKillFunction(shardKey), rp.processRecords);

  rp.initialize({
    shardId: shardKey,
    sequenceNumber: checkpoint,
  }, () => {
    // Not sure what to do here
  });
}

function lostWorker(lease: LeaseListItem, rp: RecordProcessor) {
  rp.leaseLost({}, () => {
    // Not sure what to do here
  });
}

export async function checkLeases(rp: RecordProcessor) {
  // Check that all the shards are up to date
  await shardManager.checkShards();

  await updateAllLeases();
  const expiredLeases = await getExpiredLeases();

  // Refresh the ownership of currently owned leases
  const ownedLeasesToRemove = [];
  for (const ownedLeaseId of ownedLeases) {
    const ownedLease = allLeases.find(x => x.lease.leaseKey === ownedLeaseId);

    if (!ownedLease) {
      // Can't find owned lease, most likely due to a re-shard
      ownedLeasesToRemove.push(ownedLease);
      continue;
    }

    const stillOurs = await claimLease(ownedLease.lease.leaseKey, Properties.workerId, ownedLease.lease.leaseCounter);
    if (stillOurs === false) {
      // We lost it somehow, perhaps someone stole it from us?
      ownedLeasesToRemove.push(ownedLease);
    }
  }

  // Stop workers for ownedLeasesToRemove
  for (const ownedLeaseToRemove of ownedLeasesToRemove) {
    ownedLeases = ownedLeases.filter(x => x !== ownedLeaseToRemove.lease.leaseKey);
    if (workers[ownedLeaseToRemove.lease.leaseKey]) {
      try {
        workers[ownedLeaseToRemove.lease.leaseKey].close();
        lostWorker(ownedLeaseToRemove, rp);
      } catch (err) {
        console.error(`something went wrong closing a lease`, err);
      }
    }
  }

  // Check whether or not we can claim any expired lease
  for (const expiredLease of expiredLeases) {
    if (ownedLeases.length >= MAX_LEASES_OWNED) {
      break;
    }

    const result = await claimLease(expiredLease.lease.leaseKey, expiredLease.lease.leaseOwner, expiredLease.lease.leaseCounter);
    if (result === true) {
      ownedLeases.push(expiredLease.lease.leaseKey);
      createWorker(expiredLease.lease.leaseKey, expiredLease.lease.checkpoint || null, rp);
    }
  }

  console.log('finished checking leases');
}

let semaphore = false;

export async function configure(_client: Kinesis) {
  client = _client;
  shardManager = new ShardManager(client);
}

export async function startLeaseCoordinator(rp: RecordProcessor) {
  if (semaphore === false) {
    semaphore = true;
    await checkLeases(rp);
    semaphore = false;
  }
  setInterval(async function () {
    if (semaphore === false) {
      semaphore = true;
      await checkLeases(rp);
      semaphore = false;
    }
  }, 10000);
}
