/**
 * Validator income reporter.
 */

// Import our tools.
const { readValidators, extractRewards, getLatestEpoch } = require('./lib/rewardExtractor.js');
const { saveRewards } = require('./lib/rewardSave.js');
const { chunkArray } = require('./lib/utils.js');

// If set to true then additional output will be printed to the console.
const debug = process.env.DEBUG;

// Immediately invoked async function
(async () => {

    const validators = readValidators();
    const validatorChunks = chunkArray(validators, 100); // Beaconcha.in api allows 100 validators per request.

    const epoch = await getLatestEpoch();

    let epochInterval;
    if(process.env.EPOCH_INTERVAL) {
        epochInterval = process.env.EPOCH_INTERVAL;
    }
    else {
        epochInterval = 100;
    }

    console.log("\n---------------------------\nLIBC Validator Monitor\n---------------------------\n");
    console.log('Checking every ' + epochInterval + ' epochs for new rewards.\n');

    // Every 1 minute check for new epoch.
    setInterval(async () => {
        console.log('Checking for new epoch...')
        const latestEpoch = await getLatestEpoch();
        // If the latest epoch is 100 epochs ahead of the current epoch, then we have a new epoch.
        console.log('Current epoch: ' + latestEpoch.data.epoch);
        // Add 100 to the epoch interval to account for the current epoch as an integer
        let nextCheck = parseInt(latestEpoch.data.epoch) + parseInt(epochInterval);
        console.log('Next rewards check Epoch: ' + nextCheck + '\n')
        if (latestEpoch.data.epoch >= nextCheck) {
            if(debug) {
                console.log('Checking for new rewards');
            }
            const rewards = await extractRewards(validatorChunks, epoch.data.epoch);
            saveRewards(rewards);
        }
    }, 60000);
    
})();