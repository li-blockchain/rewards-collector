const mysql = require('mysql2');
const firebase = require('firebase-admin');
const serviceAccount = require("../serviceAccountKey.json");

require('dotenv').config();

// MySQL database configuration
const mysqlConfig = {
  host: process.env.DB_HOST,
  user: process.env.DB_USERNAME,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_NAME
};

const rewards_collection = process.env.REWARD_COLLECTION;

firebase.initializeApp({
  credential: firebase.credential.cert(serviceAccount),
});

const db = firebase.firestore();
const mysqlConnection = mysql.createConnection(mysqlConfig);

// Save the rewards to the MySQL database.
async function saveRewards(rewards) {
  /*
  The rewards object is structured like this:
  {
      epoch,
      withdrawals,
      proposals,
      mev_data
  }
  We want to save a new record with the epoch number as a primary key. Each record will have withdrawals,
  proposals, and mev_data.
  */


  // Only save the record if we have withdrawals, proposals, or mev_data.
  if (rewards.withdrawals.length === 0 && rewards.proposals.length === 0) {
    console.log("No data to report");
    return;
  }

  // We are going to want to insert a record for each proposal.
  const proposalPromises = rewards.proposals.map((proposal) => {
    return new Promise((resolve, reject) => {
      const proposalQuery = 'INSERT INTO rewards (datetime, epoch, validator_index, type, reward_type, node, amount, adjusted_amount) VALUES (?, ?, ?, ?, ?, ?, ?, ?)';
      //const proposalQuery = `INSERT INTO rewards (datetime, epoch, validator_index, type, reward_type, node, amount, adjusted_amount) VALUES (null, null, null, null, 'proposal', null, null, null)`;
     
      const proposalValues = [proposal.datetime, proposal.epoch, proposal.validator_index, proposal.type, "proposal", proposal.node, proposal.amount, 0];
      
      console.log(proposalQuery);

      mysqlConnection.query(proposalQuery, proposalValues, (err, result) => {
        if (err) {
          console.error('Error saving proposal to MySQL:', err);
          reject(err);
        } else {
          console.log('Saved proposal to MySQL');
          resolve(result);
        }
      });
    });
  });

  // We are going to want to insert a record for each proposal.
  const withdrawalPromises = rewards.withdrawals.map((withdrawal) => {
    return new Promise((resolve, reject) => {
      const withdrawalQuery = 'INSERT INTO rewards (datetime, epoch, validator_index, type, reward_type, node, amount, adjusted_amount) VALUES (?, ?, ?, ?, ?, ?, ?, ?)';
      //const withdrawalQuery = `INSERT INTO rewards (datetime, epoch, validator_index, type, reward_type, node, amount, adjusted_amount) VALUES (null, null, null, null, 'withdrawal', null, null, null)`;
     
      const withdrawalValues = [withdrawal.datetime, withdrawal.epoch, withdrawal.validator_index, withdrawal.type, "withdrawal", withdrawal.node, withdrawal.amount, 0];

      mysqlConnection.query(withdrawalQuery, withdrawalValues, (err, result) => {
        if (err) {
          console.error('Error saving withdrawal to MySQL:', err);
          reject(err);
        } else {
          console.log('Saved withdrawal to MySQL');
          resolve(result);
        }
      });
    });
  });


    try {
        await Promise.all(proposalPromises);
        console.log('All proposals saved to MySQL');
    } catch (error) {
        console.error('Error saving one or more proposals to MySQL:', error);
    }

    try {
        await Promise.all(withdrawalPromises);
        console.log('All withdrawals saved to MySQL');
    } catch (error) {
        console.error('Error saving one or more withdrawals to MySQL:', error);
    }

}

module.exports = {
  saveRewards,
};
