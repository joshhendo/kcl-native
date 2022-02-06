import { Kinesis } from "aws-sdk";
import {startLeaseCoordinator} from "./lease-manager";
import {RecordProcessor} from "aws-kcl";

export function register(processor: RecordProcessor, options?: Kinesis.Types.ClientConfiguration) {
    return {
        run: () => {
            startLeaseCoordinator(processor);
        }
    }
}
