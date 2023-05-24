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
            pubkey: line[1]
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

        if(p.data?.length > 0) {
            proposals.push(p);
        }

        // Loop through each proposal and grab the mev. 
        // Endpoint: https://boost-relay.flashbots.net/relay/v1/data/bidtraces/proposer_payload_delivered?block_number=<blocknumber>
        p.data?.forEach(async (proposal) => {

            // Fetch the mev from the boost-relay.
            if(debug) {
                console.log("We have a proposal.\n")
            }

            // Loop over each MEV_RELAYS
            process.env.MEV_RELAYS.split(',').forEach(async (relay) => {
                console.log(relay);
                const url = `${relay}/relay/v1/data/bidtraces/proposer_payload_delivered?block_number=${proposal.exec_block_number}`;
                console.log(url);
                const mev = await fetch(url);
                const mevd = await mev.json();

                if(mevd.length > 0) {
                    if(debug) {
                        console.log(`We have MEV for ${proposal.exec_block_number} with ${relay}.\n}`);
                        console.log(mevd);
                    }
                    mev_data.push(mevd);
                }

            });
        })
    }

    const rewards = {
        epoch,
        withdrawals,
        proposals,
        mev_data
    }

    return rewards;
}

module.exports = {
    readValidators,
    getLatestEpoch,
    extractRewards
}