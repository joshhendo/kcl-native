import {listLeases} from "./src/dynamodb/dynamodb-lease-coordinater";
import {checkLeases, startLeaseCoordinator} from "./src/lease-manager";
import {InitializeInput, RecordProcessor} from "aws-kcl";
import KCLProcess = require("aws-kcl");
import {register} from "./src/kcl-native";
import AWS = require('aws-sdk');

(async () => {
  console.log('test');
  // const result = await listLeases();

  //console.log(result);
  //await startLeaseCoordinator();
  // await checkLeases();
  // await new Promise((resolve => setTimeout(resolve, 10050)));
  // await checkLeases();

  const client = new AWS.Kinesis({
     region: 'us-east-1',
     params: {StreamName: 'kclnodejssample'},
  });
  register(recordProcessor()).configure(client).run();
})();


function recordProcessor(): RecordProcessor {
  return {
    initialize(initializeInput: KCLProcess.InitializeInput, completeCallback: KCLProcess.Callback) {
      console.log(JSON.stringify(initializeInput));
    },
    processRecords(processRecordsInput: KCLProcess.ProcessRecordsInput, completeCallback: KCLProcess.Callback): void {
      const records = processRecordsInput.records;

      for (const record of records) {
        const fullData = new Buffer(record.data, 'base64').toString();

        console.log(JSON.stringify(fullData));

        // const parsed = JSON.parse(record.data.data.toString('utf8'))
      }

    },
    leaseLost(leaseLostInput: KCLProcess.LeaseLossInput, completeCallback: KCLProcess.Callback): void {

    },
    shardEnded(shardEndedInput: KCLProcess.ShardEndedInput, completeCallback: KCLProcess.Callback): void {
    },

  }

}
