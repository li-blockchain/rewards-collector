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

    // Read validators and chunk them up. Beaconcha.in api allows 100 validators per request.
    const validators = readValidators();
    const validatorChunks = chunkArray(validators, 100); 

    // Get our starting epoch. 
    const epoch = process.env.EPOCH_START;

    console.log("\n---------------------------\nLIBC Validator Backfill\n---------------------------\n");
    console.log("Starting on epoch: " + epoch + "\n");

    // First run.
    const rewards = await extractRewards(validatorChunks, epoch);
    saveRewards(rewards);

    // Every 1 minute check for new epoch.
    let nextEpoch = epoch;
    setInterval(async () => {
        const currentEpoch = await getLatestEpoch();
        console.log('Current epoch: ' + currentEpoch.data.epoch);
        nextEpoch = parseInt(nextEpoch) + 100;
        if(nextEpoch > currentEpoch.data.epoch) {
            console.log('Backfill Complete');
            process.exit();
        }

        console.log('Checking for new epoch: ' + nextEpoch + '\n')

        const rewards = await extractRewards(validatorChunks, nextEpoch);
        saveRewards(rewards);

    }, 60000);

})();