/***
 Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 SPDX-License-Identifier: Apache-2.0
 ***/

'use strict';

import util = require('util');
import kcl = require('aws-kcl');
import fs = require('fs');
import * as dateFns from 'date-fns';

/**
 * A simple implementation for the record processor (consumer) that simply writes the data to a log file.
 *
 * Be careful not to use the 'stderr'/'stdout'/'console' as log destination since it is used to communicate with the
 * {https://github.com/awslabs/amazon-kinesis-client/blob/master/src/main/java/com/amazonaws/services/kinesis/multilang/package-info.java MultiLangDaemon}.
 */

function writeToLogFile(data: string) {
  fs.appendFileSync(`${__dirname}/output.txt`, data + '\n');
}

function recordProcessor(): kcl.RecordProcessor {
  const log = {
    info: console.error
  };
  let shardId: string;

  return {

    initialize: function(initializeInput, completeCallback) {
      shardId = initializeInput.shardId;

      completeCallback();
    },

    processRecords: function(processRecordsInput, completeCallback) {
      if (!processRecordsInput || !processRecordsInput.records) {
        completeCallback();
        return;
      }
      var records = processRecordsInput.records;
      var record, data, sequenceNumber, partitionKey;
      for (var i = 0 ; i < records.length ; ++i) {
        record = records[i];
        data = new Buffer(record.data, 'base64').toString();
        sequenceNumber = record.sequenceNumber;
        partitionKey = record.partitionKey;
        log.info(util.format('ShardID: %s, Record: %s, SeqenceNumber: %s, PartitionKey:%s', shardId, data, sequenceNumber, partitionKey));

        const parsed = JSON.parse(data);
        const sentDate = new Date(parsed.date);
        const receivedDate = new Date();
        const differenceInMs = dateFns.differenceInMilliseconds(receivedDate, sentDate);

        writeToLogFile(data);
        writeToLogFile(`took ${differenceInMs}ms received at ${receivedDate.toISOString()}`);
      }
      if (!sequenceNumber) {
        completeCallback();
        return;
      }
      // If checkpointing, completeCallback should only be called once checkpoint is complete.
      processRecordsInput.checkpointer.checkpoint(sequenceNumber, function(err, sequenceNumber) {
        log.info(util.format('Checkpoint successful. ShardID: %s, SeqenceNumber: %s', shardId, sequenceNumber));
        completeCallback();
      });
    },

    leaseLost: function(leaseLostInput, completeCallback) {
      log.info(util.format('Lease was lost for ShardId: %s', shardId));
      completeCallback();
    },

    shardEnded: function(shardEndedInput, completeCallback) {
      log.info(util.format('ShardId: %s has ended. Will checkpoint now.', shardId));
      shardEndedInput.checkpointer.checkpoint(function() {
        completeCallback();
      });
    },

    // shutdownRequested: function(shutdownRequestedInput, completeCallback) {
    //   shutdownRequestedInput.checkpointer.checkpoint(function () {
    //     completeCallback();
    //   });
    // }
  };
}

kcl(recordProcessor()).run();
