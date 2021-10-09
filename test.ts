import {listLeases} from "./src/dynamodb/dynamodb-lease-coordinater";
import {checkLeases, startLeaseCoordinator} from "./src/lease-manager";

(async () => {
  console.log('test');
  // const result = await listLeases();

  //console.log(result);
  //await startLeaseCoordinator();
  await checkLeases();
  await new Promise((resolve => setTimeout(resolve, 10050)));
  await checkLeases();
})();
