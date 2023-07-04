/**
 * Validator income reporter.
 */

// Import our tools.
const { readValidators, extractRewards, getLatestEpoch } = require('./lib/rewardExtractor.js');
const { saveRewards } = require('./lib/rewardSave.js');
const { chunkArray } = require('./lib/utils.js');

require('dotenv').config();

// If set to true then additional output will be printed to the console.
const debug = process.env.DEBUG;

// Immediately invoked async function
(async () => {

    // Read validators and chunk them up. Beaconcha.in api allows 100 validators per request.
    const validators = readValidators();
    const validatorChunks = chunkArray(validators, 100); 

    // Get our starting epoch. 
    let startEpoch = process.env.EPOCH_WATCH_START;
    let latestEpoch = await getLatestEpoch();

    // Our epoch interval is how often we check for new rewards. Beaconcha.in goes 100 epochs at a time.
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
        latestEpoch = await getLatestEpoch();

        // If we are currently not yet at the starting epoch then don't do anything yet.
        if(latestEpoch.data.epoch < parseInt(startEpoch)) {
            console.log(`Not yet at starting epoch of ${startEpoch} Waiting...`);
            return;
        }

        // Save the rewards for latest epoch (this includes the previous `epochInterval` epochs)
        const rewards = await extractRewards(validatorChunks, latestEpoch.data.epoch);
        saveRewards(rewards);

        // Set the next starting epoch to check for rewards.
        startEpoch = parseInt(latestEpoch.data.epoch) + parseInt(epochInterval);

    }, 60000);
    
})();