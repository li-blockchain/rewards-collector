const fs = require('fs');
const path = require('path');

require('dotenv').config();

// Using the beaconcha.in api to get validator data.
const API_ENDPOINT = 'https://beaconcha.in/api/v1/';
const API_KEY = process.env.API_KEY;

// If set to true then additional output will be printed to the console.
const debug = process.env.DEBUG;

if(debug) {
    console.log("We are in verbose mode.")
}

// Loop through each line in validators.txt and create a new array entry with the index of id and pubkey
function readValidators() {
    let validators = [];
    let data = fs.readFileSync(path.join(__dirname, '../data/validators.csv'), 'utf8');
    let lines = data.split(/\r?\n/);
    for (let i = 0; i < lines.length; i++) {
        let line = lines[i].split(',');
        validators.push({
            index: line[0],
            pubkey: line[1],
            type: line[2],
            node: line[3],
            minipool: line[4]
        });
    }
    return validators;
}

// Get the validator withdrawals
const getWithdrawals = async (vindex, epoch)  => {
    let url = API_ENDPOINT + 'validator/' + vindex + '/withdrawals?epoch=' + epoch;

    // Fetch the data adding the API key to the header.
    const withdrawals = await fetch(url, {
        headers: {
            apikey: API_KEY
        }
    })
    
    return await withdrawals.json();
}

const getLatestEpoch = async () => {
    let url = API_ENDPOINT + 'epoch/finalized';

    // Fetch the data adding the API key to the header.
    const epoch = await fetch(url, {
        headers: {
            apikey: API_KEY
        }
    })

    return await epoch.json();
}

const getEpochSlots = async (epoch) => {
    let url = API_ENDPOINT + 'epoch/' + epoch + '/slots';

    console.log(url);

    // Fetch the data adding the API key to the header.
    const slots = await fetch(url, {
        headers: {
            apikey: API_KEY
        }
    })

    return await slots.json();
}

const getProposals = async (vindex,epoch) => {


    // Example: https://beaconcha.in/api/v1/validator/508675/proposals?epoch=202991
    let url = API_ENDPOINT + 'validator/' + vindex + '/proposals?epoch=' + epoch;

    // Fetch the data adding the API key to the header.
    const proposal = await fetch(url, {
        headers: {
            apikey: API_KEY
        }
    })

    return await proposal.json();
}

const extractRewards = async (validatorChunks, epoch) => {

    const withdrawals = [];
    const proposals = [];

    // Loop through all the validatorChunks
    for (let i = 0; i < validatorChunks.length; i++) {

        // Lets get all withdrawals for the last 100 epochs
        const w = await getWithdrawals(validatorChunks[i], epoch);

        if(debug) {
            console.log("Widthdrawals:\n---------------------------\n ");
            console.log(w);
        }
        
        if(w.data?.length > 0) {
            withdrawals.push(w);
        }

        // Get proposals for each validator
        const p = await getProposals(validatorChunks[i], epoch);

        if(debug) {
            console.log("Proposals:\n---------------------------\n ");
            console.log(p);
        }

        // Loop through each proposal and grab the mev. 
        // Supports multiple MEV_RELAYS
        if(p.data?.length > 0) {
            for (let i = 0; i < p.data.length; i++) {
                console.log(`Checking block # for ${p.data[i].exec_block_number}`);
                //console.log(p.data[i].exec_block_number);

                // If we don't have a block # then skip this proposal.
                if(!p.data[i].exec_block_number) {
                    console.log("No block number found for proposal. Skipping.");
                    console.log(p.data[i]);

                    continue;
                }

                // Get the block proposal rewards from the following endpoint: https://beaconcha.in/api/v1/execution/block/17403189
                const block = await fetch(`https://beaconcha.in/api/v1/execution/block/${p.data[i].exec_block_number}`);
                const block_data = await block.json();

                // console.log(block_data.data[0].blockMevReward);
                // console.log(block_data.data[0].producerReward);
                // console.log(block_data.data[0].posConsensus.proposerIndex);

                // check if .relay is a defined property
                if (block_data.data[0].relay) {
                    console.log(block_data.data[0].relay.tag);

                    proposals.push({
                        source: block_data.data[0].relay.tag,
                        value: block_data.data[0].blockMevReward,
                        block_number: p.data[i].exec_block_number,
                        datetime: block_data.data[0].timestamp,
                        proposer: block_data.data[0].posConsensus.proposerIndex,
                        epoch: block_data.data[0].posConsensus.epoch
                    });
                }
                else {
                    proposals.push({
                        source: '',
                        value: block_data.data[0].producerReward,
                        block_number: p.data[i].exec_block_number,
                        datetime: block_data.data[0].timestamp,
                        proposer: block_data.data[0].posConsensus.proposerIndex,
                        epoch: block_data.data[0].posConsensus.epoch
                    });
                }
              }
        }
       
    }

    const rewards = {
        epoch,
        withdrawals,
        proposals
    }

    // Clean up the data and return it.
    return cleanRewards(rewards);
}


const cleanRewards = async (rewards) => {
    // We are going to take the separate fields we want to track. 

    // First lets get the list of validators indexed by the validator index.
    // Save the validators to the database.
    const validators = readValidators();

    // Convert validators to be indexed by the index key in the object.
    const validatorsByIndex = {};
    validators.forEach(validator => {
        validatorsByIndex[validator.index] = validator;
    });

    // We want withdrawals data in for the following format: {validator_index, amount, datetime, epoch}
    let cleaned_withdrawals = [];
    if(rewards.withdrawals.length > 0) {
        
        console.log("Getting slots for epoch: " + rewards.withdrawals[0].data[0].epoch);
        const slots = await getEpochSlots(rewards.withdrawals[0].data[0].epoch);

         cleaned_withdrawals =  await Promise.all(rewards.withdrawals[0].data.map(async (w) => {
            // The the last slot for the epoch. Doing this just to get the timestamp.
            const last_slot = slots.data[slots.data.length - 1];

            return {
                validator_index: w.validatorindex,
                amount: w.amount,
                type: validatorsByIndex[w.validatorindex].type,
                node: validatorsByIndex[w.validatorindex].node,
                minipool: validatorsByIndex[w.validatorindex].minipool,
                datetime: last_slot.exec_timestamp,
                epoch: w.epoch
            }
        }))
    }

    // Lets give the proposals a similar treatment.
    let cleaned_proposals = [];
    if (rewards.proposals.length > 0) {
        cleaned_proposals = await Promise.all(rewards.proposals.map(async (p) => {
            // Get Epoch slots.
            //const slots = await getEpochSlots(p.epoch);

            // The the last slot for the epoch. Doing this just to get the timestamp.
            // const last_slot = slots.data[slots.data.length - 1];

            return {
                validator_index: p.proposer,
                amount: p.value,
                mev_source: p.source,
                datetime: p.datetime,
                exec_block_number: p.block_number,
                type: validatorsByIndex[p.proposer].type,
                node: validatorsByIndex[p.proposer].node,
                minipool: validatorsByIndex[p.proposer].minipool,
                epoch: p.epoch
            };
        }));
    }

    return {
        start_epoch: rewards.epoch,
        withdrawals: cleaned_withdrawals,
        proposals: cleaned_proposals
    }
    
}


module.exports = {
    readValidators,
    getLatestEpoch,
    extractRewards
}