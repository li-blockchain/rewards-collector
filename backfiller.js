/**
 * Validator income reporter.
 * This is used to capture historical rewards for validators.
 */

// Import our tools.
const { readValidators, extractRewards, getLatestEpoch } = require('./lib/rewardExtractor.js');
const { saveRewards } = require('./lib/rewardSave.js');
const { chunkArray } = require('./lib/utils.js');

// If set to true then additional output will be printed to the console.
const debug = process.env.DEBUG;

// Immediately invoked async function
(async () => {

    // We want to track how long this process takes.
    const start = Date.now();

    // Read validators and chunk them up. Beaconcha.in api allows 100 validators per request.
    const validators = readValidators();
    const validatorChunks = chunkArray(validators, 100); 

    // Get our starting epoch. 
    const epoch = process.env.EPOCH_START;

    console.log("\n---------------------------\nLIBC Validator Backfill\n---------------------------\n");
    console.log("Starting on epoch: " + epoch + "\n");

    // First run.
    const rewards = await extractRewards(validatorChunks, epoch);
    await saveRewards(rewards);

    // Every 1 minute check for new epoch.
    let nextEpoch = epoch;
    setInterval(async () => {
        const currentEpoch = await getLatestEpoch(0);
        //console.log('Current epoch: ' + currentEpoch.data.epoch);
        nextEpoch = parseInt(nextEpoch) + 100;
        if(nextEpoch > currentEpoch.data.epoch && currentEpoch.data.epoch > 0) {
            console.log('Backfill Complete on Epoch ' + (nextEpoch-100) + '!');
            console.log('Total time: ' + ((Date.now() - start) / 1000) + ' seconds');
            process.exit();
        }

        console.log('Collecting rewards for: ' + nextEpoch + '\n')

        const rewards = await extractRewards(validatorChunks, nextEpoch);

        await saveRewards(rewards);

    }, 60000);

})();
