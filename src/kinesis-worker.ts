import AWS = require('aws-sdk');
import {checkpoint} from "./dynamodb/dynamodb-lease-coordinater";
const KinesisReadable = require('kinesis-readable')

export function startWorker(shardId: string, startSequence: string | null, killFn: () => void, runFn: (data: any) => void) {
  const client = new AWS.Kinesis({
    region: 'us-east-1',
    params: {StreamName: 'kclnodejssample'},
  });

  const options: any = {
    shardId: shardId,
    limit: 100
  };

  if (startSequence === 'SHARD_END') {
    // Log an error? Attempted to start a worker on a shard that's ended.
    return;
  }

  if (startSequence == null || startSequence === 'TRIM_HORIZON') {
    options.iterator = 'TRIM_HORIZON';
  } else {
    options.startAfter = startSequence;
    // options.iterator = 'TRIM_HORIZON';
  }

  const readable = KinesisReadable(client, options);
  let shardEnded = false;

  readable.on('data', async function(records: any) {
    for (const record of records) {
      await runFn(record);
    }

    // Need to checkpoint this
    if (shardEnded) {
      return;
    }

    const sequenceNumberForCheckpoint = records[records.length-1].SequenceNumber;
    const checkpointResult = await checkpoint(shardId, sequenceNumberForCheckpoint);
    if (checkpointResult === false) {
      // Something went wrong. Maybe we lost the checkpoint
      readable.close();
    }
  })
    .on('checkpoint', function(data: any) {
      console.log(`got checkpoint event`, data);
    })
    .on('error', async function(err: any) {
      if (err.code === 'MissingRequiredParameter' && err.message === "Missing required key 'ShardIterator' in params") {
        shardEnded = true;
        await checkpoint(shardId, 'SHARD_END');
        readable.close();
        return;
      }

      console.error(err);
    })
    .on('end', function() {
      killFn();
      console.log('all done');
    })

  return readable;
}
