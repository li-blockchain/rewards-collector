const fs = require('fs');
const path = require('path');

// Using the beaconcha.in api to get validator data.
const API_ENDPOINT = 'https://beaconcha.in/api/v1/';
const API_KEY = process.env.API_KEY

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

    console.log(url)

    // Fetch the data adding the API key to the header.
    const proposal = await fetch(url, {
        headers: {
            apikey: API_KEY
        }
    })

    return await proposal.json();
}

module.exports = {
    readValidators,
    getWithdrawals,
    getLatestEpoch,
    getProposals
}