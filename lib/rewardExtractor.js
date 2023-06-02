const fs = require('fs');
const path = require('path');

require('dotenv').config();

// Using the beaconcha.in api to get validator data.
const API_ENDPOINT = 'https://beaconcha.in/api/v1/';
const API_KEY = process.env.API_KEY;

// If set to true then additional output will be printed to the console.
const debug = process.env.DEBUG;

if(debug === true) {
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
    const mev_data = [];

    // Loop through all the validatorChunks
    for (let i = 0; i < validatorChunks.length; i++) {

        // Lets get all withdrawals for the last 100 epochs
        const w = await getWithdrawals(validatorChunks[i], epoch);

        if(debug === true) {
            console.log("Widthdrawals:\n---------------------------\n ");
            console.log(w);
        }
        
        if(w.data?.length > 0) {
            withdrawals.push(w);
        }

        // Get proposals for each validator
        const p = await getProposals(validatorChunks[i], epoch);

        if(debug === true) {
            console.log("Proposals:\n---------------------------\n ");
            console.log(p);
        }

        // Loop through each proposal and grab the mev. 
        // Supports multiple MEV_RELAYS
        if(p.data?.length > 0) {
            for(let i = 0; i < p.data.length; i++) {
                // Loop over each MEV_RELAYS
                let mevd = null;
                process.env.MEV_RELAYS.split(',').forEach(async (relay) => {
                    if(!mevd) {
                        const url = `${relay}/relay/v1/data/bidtraces/proposer_payload_delivered?block_number=${p.data[i].exec_block_number}`;
                        try {
                            const mev = await fetch(url);
                            mevd = await mev.json();

                            if(mevd.length > 0) {
                                if(debug === true) {
                                    console.log(`We have MEV for ${p.data[i].exec_block_number} with ${relay}.\n}`);
                                    console.log(mevd);
                                }
                                mevd.source = relay;       // Save the source of the mev.
                                p.data[i].mev = mevd;      // Add the mev to the proposal.
                                proposals.push(p.data[i]);
                                mev_data.push(mevd);
                                return;
                            }
                        }
                        catch (e) {
                            console.log(e);
                            console.log(url);
                        }
                    }
                });
            }
        }
       
    }

    const rewards = {
        epoch,
        withdrawals,
        proposals,
        mev_data
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
            const slots = await getEpochSlots(p.epoch);

            // The the last slot for the epoch. Doing this just to get the timestamp.
            const last_slot = slots.data[slots.data.length - 1];

            return {
                validator_index: p.proposer,
                amount: p.mev[0].value,
                mev_source: p.mev.source,
                datetime: last_slot.exec_timestamp,
                exec_block_number: p.exec_block_number,
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