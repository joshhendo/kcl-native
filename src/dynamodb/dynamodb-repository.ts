import * as AWS from 'aws-sdk';
import {Properties} from "../properties-manager";
import {DynamoDB} from "aws-sdk";
import {ConditionExpression} from "aws-sdk/clients/dynamodb";

AWS.config.update({
  region: 'us-east-1',
});

const DynamoDbClient = new AWS.DynamoDB.DocumentClient();

export interface Lease {
  leaseKey: string;
  leaseOwner: string;
  checkpoint: string;
  leaseCounter: number;
  checkpointSubSequenceNumber: number;
  ownerSwitchesSinceCheckpoint: number;
}

export async function getAllRecords(): Promise<Lease[]> {
  const params = {
    TableName: Properties.applicationName,
  };

  try {
    const records = await DynamoDbClient.scan(params).promise();

    if (!records.Items || records.Items.length === 0) {
      return [];
    }

    return records.Items as Lease[];
  } catch(err) {
    throw err;
  }
}

export async function updateLeaseOwner(shardId: string, currentLeaseOwner: string, currentLeaseCounter: number): Promise<any> {
  const params = {
    TableName: Properties.applicationName,
    Key: {
      'leaseKey': shardId,
    },
    UpdateExpression: 'SET leaseOwner = :newLeaseOwner ADD leaseCounter :inc',
    ConditionExpression: '#leaseOwner = :existingLeaseOwner AND #leaseCounter = :leaseCounter',
    ExpressionAttributeNames: {
      '#leaseOwner': 'leaseOwner',
      '#leaseCounter': 'leaseCounter',
    },
    ExpressionAttributeValues: {
      ':existingLeaseOwner': currentLeaseOwner,
      ':leaseCounter': currentLeaseCounter,
      ':newLeaseOwner': Properties.workerId,
      ':inc': 1,
    }
  };

  if (currentLeaseOwner == null) {
    params.ConditionExpression = 'attribute_not_exists(leaseOwner)';
    delete params.ExpressionAttributeNames;
    delete params.ExpressionAttributeValues[':existingLeaseOwner'];
    delete params.ExpressionAttributeValues[':leaseCounter'];
  }

  await DynamoDbClient.update(params).promise();
}

export async function updateCheckpoint(shardId: string, checkpoint: string) {
  const params = {
    TableName: Properties.applicationName,
    Key: {
      'leaseKey': shardId,
    },
    UpdateExpression: 'SET checkpoint = :checkpoint',
    ConditionExpression: '#leaseOwner = :existingLeaseOwner',
    ExpressionAttributeNames: {
      '#leaseOwner': 'leaseOwner',
    },
    ExpressionAttributeValues: {
      ':existingLeaseOwner': Properties.workerId,
      ':checkpoint': checkpoint,
    }
  };

  await DynamoDbClient.update(params).promise();
}

export async function createNewShardRecord(shardId: string, parentShardId: string[]) {
  try {
    const params = {
      TableName: Properties.applicationName,
      ConditionExpression: 'attribute_not_exists(leaseKey)',
      Item: {
        leaseKey: shardId,
        checkpoint: 'TRIM_HORIZON',
        leaseCounter: 0,
        checkpointSubSequenceNumber: '0',
        ownerSwitchesSinceCheckpoint: 0,
        ...(parentShardId.length) && ({parentShardId: DynamoDbClient.createSet(parentShardId)}),
      }
    };

    await DynamoDbClient.put(params).promise();
  } catch (err) {
    // Most likely a concurrency issue
    console.error(`error creating new shard record`, err);
  }
}
