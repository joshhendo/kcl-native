import { Kinesis } from "aws-sdk";

type ProcessFunction = (data: any) => void;

export async function register(processor: ProcessFunction, options?: Kinesis.Types.ClientConfiguration) {

}
