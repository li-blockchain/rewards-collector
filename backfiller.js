/**
 * Validator income reporter.
 * This is used to capture historical rewards for validators.
 */

// Import our tools.
const { readValidators, extractRewards, getLatestEpoch } = require('./lib/rewardExtractor.js');
const { saveRewards } = require('./lib/rewardSave.js');
const { chunkArray } = require('./lib/utils.js');
const fs = require('fs');

// If set to true then additional output will be printed to the console.
const debug = process.env.DEBUG;

// Immediately invoked async function
(async () => {

    // We want to track how long this process takes.
    const start = Date.now();

    // Read validators and chunk them up. Beaconcha.in api allows 100 validators per request.
    const validators = readValidators();
    const validatorChunks = chunkArray(validators, 100);

    // If the data/.lastepoch file exists then we want to start from there.
    // This is useful if the process is interrupted.
    // If the file does not exist then we will start from the EPOCH_START env variable.

    if(fs.existsSync('./data/.lastepoch')) {
        const lastEpoch = fs.readFileSync('./data/.lastepoch', 'utf8');

        // validate lastEpoch is a number.
        if(isNaN(lastEpoch)) {
            console.log('Error: .lastepoch file parse error. Not a valid epoch number.');
            process.exit();
        }

        console.log('Using .lastepoch file: ' + lastEpoch);
        process.env.EPOCH_START = lastEpoch;
    }

    // Get our starting epoch. 
    const epoch = process.env.EPOCH_START

    console.log("\n---------------------------\nLIBC Validator Backfill\n---------------------------\n");
    console.log("Starting on epoch: " + epoch + "\n");

    // First run.
    const rewards = await extractRewards(validatorChunks, epoch);
    saveRewards(rewards);

    // Every 1 minute check for new epoch.
    let nextEpoch = epoch;
    setInterval(async () => {
        const currentEpoch = await getLatestEpoch(0);
        console.log('Current epoch: ' + currentEpoch.data.epoch);
        nextEpoch = parseInt(nextEpoch) + 100;
        if(nextEpoch > currentEpoch.data.epoch && currentEpoch.data.epoch > 0) {
            // Save nextEpoch to data/.lastepoch file
            fs.writeFileSync('./data/.lastepoch', String(nextEpoch));

            console.log('Backfill Complete on Epoch ' + (nextEpoch-100) + '!');
            console.log('Total time: ' + ((Date.now() - start) / 1000) + ' seconds');
            process.exit();
        }

        console.log('Collecting rewards for: ' + nextEpoch + '\n')

        const rewards = await extractRewards(validatorChunks, nextEpoch);
        await saveRewards(rewards);

    }, 10000);

})();
