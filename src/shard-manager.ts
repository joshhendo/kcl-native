import AWS = require("aws-sdk");
import {ShardList} from "aws-sdk/clients/kinesis";
import {createNewShardRecord, getAllRecords, updateCheckpoint} from "./dynamodb/dynamodb-repository";
import {Properties} from "./properties-manager";

export class ShardManager {
  client: AWS.Kinesis;

  constructor(client: AWS.Kinesis) {
    this.client = client;
  }

  // TODO: Handle NextToken from original response
  private listShards(): Promise<ShardList> {
    return new Promise((resolve, reject) => {
      this.client.listShards((err, data) => {
        if (err) {
          return reject(err);
        }
        return resolve(data.Shards);
      })
    });
  }

  public async checkShards() {
    const shards = await this.listShards();

    // console.log(`shards: ${JSON.stringify(shards)}`);

    const records = await getAllRecords();

    // Check for any shards that have reached the end checkpoint
    for (const record of records) {
      // Only care about records that we have a lease on
      if (record.leaseOwner !== Properties.workerId) {
        continue;
      }

      const shard = shards.find(x => x.ShardId === record.leaseKey);
      if (!shard) {
        continue;
      }

      if (shard.SequenceNumberRange.EndingSequenceNumber == null) {
        continue;
      }

      if (shard.SequenceNumberRange.EndingSequenceNumber <= record.checkpoint) {
        try {
          await updateCheckpoint(record.leaseKey, 'SHARD_END');
          // Implement this:
          // "Deleting lease for shard shardId-000000000019 as it has been completely processed and processing of child shards has begun."
        } catch (err) {
          console.error(`error ending shard`);
          // Possibly some concurrency issue?
        }
      }
    }

    // Check for any shards that don't exist in Dynamo yet
    for (const shard of shards) {
      const record = records.find(x => x.leaseKey === shard.ShardId);
      if (record) {
        continue;
      }

      // A record for the shard doesn't exist. Make sure it isn't closed
      if (shard.SequenceNumberRange.EndingSequenceNumber != null) {
        continue;
      }

      // Likely to be some concurrency issues.
      // As such, we use `attribute_not_exist(leaseKey)` just in case a parallel worker tries the same thing.
      const parentShardId = [shard.ParentShardId, shard.AdjacentParentShardId].filter(x => !!x);

      await createNewShardRecord(shard.ShardId, parentShardId);
    }
  }
}
