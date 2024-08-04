/**
 * Validator income reporter.
 * This is used to capture historical rewards for validators.
 */

// Import our tools.
const { readValidators, extractRewards, getLatestEpoch } = require('./lib/rewardExtractor.js');
const { saveRewards } = require('./lib/rewardSave.js');
const { chunkArray } = require('./lib/utils.js');
const fs = require('fs');
const path = require('path');

require('dotenv').config()

// If set to true then additional output will be printed to the console.
const debug = process.env.DEBUG;

const { Client, GatewayIntentBits, Events } = require('discord.js');

const client = new Client({ intents: [GatewayIntentBits.Guilds] });

client.login(process.env.DISCORD_KEY);

async function sendMessageToChannel(channelId, message) {
    try {
        const channel = await client.channels.fetch(channelId);
        if (!channel) {
            console.error(`Channel not found for ID: ${channelId}`);
            return;
        }
        await channel.send(message);
    } catch (error) {
        console.error(`Error sending message to channel ${channelId}:`, error);
    }
}

client.once('ready', () => {
    console.log('Discord bot is ready!');
    // Immediately invoked async function
    (async () => {

        // We want to track how long this process takes.
        const start = Date.now();

        await sendMessageToChannel(process.env.DISCORD_CHANNEL, 'Getting some reward data.');

        // Read validators and chunk them up. Beaconcha.in api allows 100 validators per request.
        const validators = readValidators();
        const validatorChunks = chunkArray(validators, 100);

        // If the data/.lastepoch file exists then we want to start from there.
        // This is useful if the process is interrupted.
        // If the file does not exist then we will start from the EPOCH_START env variable.
	
	// Assuming DATA_DIR is an environment variable holding the directory path
	const dataDir = process.env.DATA_DIR;

	// Construct the full file path
	const filePath = path.join(dataDir, '.lastepoch');

        if(fs.existsSync(filePath)) {
            const lastEpoch = fs.readFileSync(filePath, 'utf8');

            // validate lastEpoch is a number.
            if(isNaN(lastEpoch)) {
                console.log('Error: .lastepoch file parse error. Not a valid epoch number.');
                process.exit();
            }

            console.log('Using .lastepoch file: ' + lastEpoch);
            process.env.EPOCH_START = lastEpoch;
        }
	else {
	   console.log("No .lastepoch found");
           console.log(filePath);
	}

        // Get our starting epoch. 
        const epoch = process.env.EPOCH_START

        console.log("\n---------------------------\nLIBC Validator Backfill\n---------------------------\n");
        console.log("Starting on epoch: " + epoch + "\n");

        // First run.
        //const rewards = await extractRewards(validatorChunks, epoch);
        //saveRewards(rewards);

        // Every 1 minute check for new epoch.
        let nextEpoch = epoch;
        setInterval(async () => {
            const currentEpoch = await getLatestEpoch(0);
            console.log('Current epoch: ' + currentEpoch.data.epoch);
            if (!currentEpoch || !currentEpoch.data) {
		 console.error('Failed to retrieve current epoch data.');
            	// You might want to retry or take some other action here
            	return; // Skip this iteration
	    }		
            nextEpoch = parseInt(nextEpoch) + 100;
            if(nextEpoch > currentEpoch.data.epoch && currentEpoch.data.epoch > 0) {
                // Save nextEpoch to data/.lastepoch file
                fs.writeFileSync(filePath, String(nextEpoch-100));

                console.log('Backfill Complete on Epoch ' + (nextEpoch-100) + '!');
                console.log('Total time: ' + ((Date.now() - start) / 1000) + ' seconds');

                //await sendMessageToChannel('1055560295898152970', 'Reward extraction complete.');
                await sendMessageToChannel(process.env.DISCORD_CHANNEL, 'Backfill Complete on Epoch ' + (nextEpoch-100) + '!');
                await sendMessageToChannel(process.env.DISCORD_CHANNEL, 'Total time: ' + ((Date.now() - start) / 1000) + ' seconds');
                process.exit();
            }

            console.log('Collecting rewards for: ' + nextEpoch + '\n')

            const rewards = await extractRewards(validatorChunks, nextEpoch);
            await saveRewards(rewards);

        }, 60000);

    })();
});
