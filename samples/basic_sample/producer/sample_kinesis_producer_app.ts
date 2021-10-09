/***
 Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 SPDX-License-Identifier: Apache-2.0
 ***/

'use strict';

import AWS = require('aws-sdk');
import config = require('./config');
import producer = require('./sample_producer');

const kinesis = new AWS.Kinesis({
  region: config.default.kinesis.region,
  // endpoint: 'http://localhost:4567'
});
producer.sampleProducer(kinesis, config.default.sampleProducer).run();
