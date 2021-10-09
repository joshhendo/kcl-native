/***
 Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 SPDX-License-Identifier: Apache-2.0
 ***/

'use strict';

const config = {
  kinesis : {
    region : 'us-east-1'
  },

  sampleProducer : {
    stream : 'kclnodejssample',
    shards : 2,
    waitBetweenDescribeCallsInSeconds : 5
  }
};

export default config;
