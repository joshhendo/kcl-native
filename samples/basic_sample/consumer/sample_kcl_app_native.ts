import AWS = require('aws-sdk');
const KinesisReadable = require('kinesis-readable');
const Kinesalite = require('kinesalite');

(async () => {
  // await new Promise((resolve, reject) => {
  //   const kinesaliteServer = Kinesalite('./kinesis-db');
  //   kinesaliteServer.listen(4567, function(err: any) {
  //     if (err) {
  //       return reject(err);
  //     }
  //
  //     resolve(null);
  //   });
  // });


  // const client = new AWS.Kinesis({
  //   region: 'us-east-1',
  //   params: { StreamName: 'kclnodejssample' }
  // });

  const kinesis = new AWS.Kinesis({
    region: 'us-east-1',
    // endpoint: 'http://localhost:4567'
  });

  // await new Promise((resolve, reject) => {
  //   kinesis.createStream({ShardCount: 1, StreamName: 'kclnodejssample'}, (err => {
  //     resolve(null);
  //   }));
  // });

  await new Promise(((resolve, reject) => {
    kinesis.listStreams((err, data) => {
      resolve(null);
    })
  }));

  const client = new AWS.Kinesis({
    region: 'us-east-1',
    params: { StreamName: 'kclnodejssample' },
    // endpoint: 'http://localhost:4567'
  });

  const shards: any = await new Promise((resolve, reject) => {
    client.listShards((err, data) => {
      if (err) {
        return reject(err);
      }
      return resolve(data);
    })
  })

  const options = {
    shardId: shards.Shards[0].ShardId,
    iterator: 'TRIM_HORIZON',
    limit: 100
  }

  const readable = KinesisReadable(client, options);

  readable
    .on('data', function(records: any) {
      console.log(records);

      // Should checkpoint here

    })
    .on('error', function(err: any) {
      console.error(err);
    })
    .on('end', function() {
      console.log('all done');
    });

  setTimeout(function() {
    readable.close();
  }, 60 * 60 * 1000);
})();


