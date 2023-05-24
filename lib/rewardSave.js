// Functions to save the extracted rewards to the database.

const firebase = require('firebase-admin');
const serviceAccount = require("../serviceAccountKey.json");
const { convertNestedArrays } = require('./utils');

firebase.initializeApp({
    credential: firebase.credential.cert(serviceAccount),
});

const db = firebase.firestore();

// Save the rewards to the database.
async function saveRewards(rewards) {
    /*
    The rewards object is structured like this:
    {
        epoch,
        withdrawals,
        proposals,
        mev_data
    }   
    We want to save a new document with the epoch number as the document name. Each document will have withdrawls,
    proposals, and mev_data.
    */

    // Only save the document if we have withdrawals, proposals, or mev_data.
    if(rewards.withdrawals.length == 0 && rewards.proposals.length == 0 && rewards.mev_data.length == 0) {
        return;
    }

    // Create the document.
    const docRef = db.collection('rewards').doc(rewards.epoch.toString());

    const r = convertNestedArrays(rewards);
    // Save the document
    const res = await docRef.set(r);

    return res;
}

module.exports = {
    saveRewards
}