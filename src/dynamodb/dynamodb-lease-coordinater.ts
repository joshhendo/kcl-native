
// https://github.com/singular-labs/amazon-kinesis-client/blob/acc61ea41dcc83b90e9d752eb555302503a30891/amazon-kinesis-client/src/main/java/software/amazon/kinesis/leases/dynamodb/DynamoDBLeaseTaker.java#L251

import {getAllRecords, Lease, updateCheckpoint, updateLeaseOwner} from "./dynamodb-repository";

export async function listLeases(): Promise<Lease[]> {
  return getAllRecords();
}

export async function claimLease(shardId: string, currentOwner: string, currentLeaseCounter: number): Promise<boolean> {
  try {
    await updateLeaseOwner(shardId, currentOwner, currentLeaseCounter);
    return true;
  } catch (err) {
    return false;
  }
}

export async function checkpoint(shardId: string, checkpoint: string) {
  try {
    await updateCheckpoint(shardId, checkpoint);
    return true;
  } catch (err) {
    return false;
  }
}
