/**
 * Validator income reporter.
 */

// Import our tools.
const { readValidators, getWithdrawals, getLatestEpoch, getProposals } = require('./lib/rewardExtractor.js');
const { chunkArray } = require('./lib/utils.js');


// Immediatly invoked async function
(async () => {

    const validators = readValidators();
    const chunks = chunkArray(validators, 100); // Beaconcha.in api allows 100 validators per request.

    // Build an array of CSV strings for each chunk.
    const validatorChunks = chunks.map((chunk) => {
        let validatorList = '';
        // For each validators get the withdrawals
        for (let i = 0; i < chunk.length; i++) {
            // Build a comma seperated list of validators.
            validatorList += chunk[i].index + ',';
        }

        // Remove the last comma
        validatorList = validatorList.slice(0, -1);
        return validatorList;
    });

    const epoch = await getLatestEpoch();

    console.log('Latest epoch: ' + epoch.data.epoch);

    // Loop through all the validatorChunks
    for (let i = 0; i < validatorChunks.length; i++) {

        // Lets get all withdrawals for the last 100 epochs
        const withdrawals = await getWithdrawals(validatorChunks[i], epoch.data.epoch);

        console.log("Widthdrawals:\n ");
        console.log(withdrawals);

        // Get proposals for each validator
        console.log("Proposals:\n ");
        const proposals = await getProposals(validatorChunks[i], epoch.data.epoch);
        console.log(proposals);

        // Loop through each proposal and grab the mev. 
        // Endpoint: https://boost-relay.flashbots.net/relay/v1/data/bidtraces/proposer_payload_delivered?block_number=<blocknumber>
        let mev_total = 0;
        proposals.data.forEach(async (proposal) => {
            console.log("We have a proposal.")
            // Fetch the mev from the boost-relay.
            const url = 'https://boost-relay.flashbots.net/relay/v1/data/bidtraces/proposer_payload_delivered?block_number=' + proposal.exec_block_number;
            console.log(url)
            const mev = await fetch(url)
            const mev_data = await mev.json();
            console.log(mev_data)
        })
    }
})();