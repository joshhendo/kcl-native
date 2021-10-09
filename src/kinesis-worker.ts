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

  if (startSequence == null) {
    options.iterator = 'TRIM_HORIZON';
  } else {
    options.startAfter = startSequence;
  }

  const readable = KinesisReadable(client, options);

  readable.on('data', async function(records: any) {
    for (const record of records) {
      await runFn(record);
    }

    // Need to checkpoint this
    const sequenceNumberForCheckpoint = records[records.length-1].SequenceNumber;
    const checkpointResult = await checkpoint(shardId, sequenceNumberForCheckpoint);
    if (checkpointResult === false) {
      // Something went wrong. Maybe we lost the checkpoint
      readable.close();
    }
  })
    .on('error', function(err: any) {
      console.error(err);
    })
    .on('end', function() {
      killFn();
      console.log('all done');
    })

  return readable;
}
