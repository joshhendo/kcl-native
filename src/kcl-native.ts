import { Kinesis } from "aws-sdk";
import {configure, startLeaseCoordinator} from "./lease-manager";
import {RecordProcessor} from "aws-kcl";

export function register(processor: RecordProcessor, options?: Kinesis.Types.ClientConfiguration) {

    const result = {
        configure: (client: Kinesis) => {
            configure(client);
            return result;
        },
        run: () => {
            startLeaseCoordinator(processor);
        }
    }

    return result;
}
