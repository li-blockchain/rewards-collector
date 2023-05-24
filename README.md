# Validator Performance Aggregator 

Leveraging the Beaconcha.in api and a list of validators, we extract rewards every 100 epochs. This includes CC withdrawals and EC block proposal rewards.

To use this place a file named "validators.csv" in the data directory. The file should have the following format with no headers:

```Index, Pubkey, Type, Node, Minipool address```

We really only need the Index currently. 

Some configuration options you can set in your environment: 

#### DEBUG = true || false  
This will output additional details regarding the various API calls and results from those calls.

#### EPOCH_START = integer
The epoch we should start checking for withdrawals and proposals.

#### EPOCH_INTERVAL = integer
This will be the number of epochs to wait until trying to extract reward data. Beaconcha.in uses 100.

For a full listing of configuration options see `.env.dist`. Copy this to `.env` and fill in with your beaconcha.in api key.
