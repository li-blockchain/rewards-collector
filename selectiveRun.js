// Script to run for any block of epochs.

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

    console.log("\n---------------------------\nLIBC Validator Single Runner\n---------------------------\n");

    // Get the epoch number from the command line argument
    const epochNumber = process.argv[2]; // Assuming the epoch number is provided as the first argument

    if (!epochNumber) {
        console.error('Please provide the epoch number as a command line argument.');
        return;
    }

    // Save the rewards for latest epoch (this includes the previous `epochInterval` epochs)
    const rewards = await extractRewards(validatorChunks, epochNumber);
    saveRewards(rewards);
        
})();