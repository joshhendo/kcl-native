import {register} from "../../src/kcl-native";


describe('integration: pubsub', function () {
    this.timeout(10000);

    it('is able to consume a single message', async function () {
        setTimeout(() => {
            console.log('should publish some sample data...');
        }, 1000);

        await new Promise((resolve, reject) => {
            const processor = (data: any) => {
                console.log('got some data!');
                console.log(data);
                resolve(data);
            }

            // register(processor);
        });
    })
});
