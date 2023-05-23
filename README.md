#Validator Performance Aggregator. 

Leveraging the Beaconcha.in api and a list of validators, we extract rewards every 100 epochs. This includes CC withdrawals and EC block proposal rewards.

To use this place a file named "validators.csv" in the data directory. The file should have the following format with no headers:

```Index, Pubkey, Type, Node, Minipool address```

We really only need the Index currently. 
