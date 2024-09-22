// This script will go out and collect data on minipools that are associated with a rocketpool node.
const { publicClient } = require("./config.js");
const { RocketStorageABI } = require("./abis/RocketStorage.js");
const { RocketMinipoolManagerABI } = require("./abis/RocketMinipoolManager.js");
const { minipoolDelegateAbi } = require("./abis/MinipoolDelegate.js");
const { rocketNodeStakingAbi } = require("./abis/RocketNodeStaking.js");
const { nodeDistributorDelegateAbi } = require("./abis/NodeDistributorDelegate.js");
const { getContract, keccak256 } = require("viem");
const firebase = require("firebase-admin");

const serviceAccount = require("./serviceAccountKey.json");

firebase.initializeApp({
    credential: firebase.credential.cert(serviceAccount),
});

const db = firebase.firestore();

// Load environment variables from .env file
require('dotenv').config();

// Parse the JSON string into an array of nodes
const nodes = JSON.parse(process.env.NODES);

// Lets define the address of the rocektpool storage contract.
const rplStorageContractAddress = "0x1d8f8f00cfa6758d7bE78336684788Fb0ee0Fa46";

const storageContract = getContract({
    address: rplStorageContractAddress,
    abi: RocketStorageABI,
    client: publicClient,
});

// Get the contract addresses from the storage contract.
const getContractAddress = async (contractName) => {
    try {
        const contractAddress = await storageContract.read.getAddress([keccak256(contractName)]);
        return contractAddress;
    } catch (error) {
        console.error(`Error fetching ${contractName} contract address:`, error);
        throw error;
    }
};

const getMinipools = async (node) => {
    const minipoolManagerAddress = await getContractAddress("contract.addressrocketMinipoolManager");
    
    const minipoolManagerContract = getContract({
        address: minipoolManagerAddress,
        abi: RocketMinipoolManagerABI,
        client: publicClient,
    });

    // Get the count of minipools for this node.
    const minipoolCount = await minipoolManagerContract.read.getNodeMinipoolCount([node]);
    console.log(`Node ${node} has ${minipoolCount} minipools.`);

    // Now we want to loop and build an array of minipool addresses.
    const minipoolAddresses = [];
    for (let i = 0; i < minipoolCount; i++) {
        const minipoolAddress = await minipoolManagerContract.read.getNodeMinipoolAt([node, i]);
        minipoolAddresses.push(minipoolAddress);
    }

    return minipoolAddresses;
};

// Convert BigInt to String for storage
const bigIntToString = (value) => value.toString();

// Get the node manager contract address
(async () => {
    try {
        // 1. Get contract addresses we will need.
        const nodeManagerAddress = await getContractAddress("contract.addressrocketNodeManager");
        const rocketpoolNodeStakingAddress = await getContractAddress("contract.addressrocketNodeStaking");

        // 2. Loop through each node.
        for (const node of nodes) {
            // 2.1 Get the node's minipools
            const minipools = await getMinipools(node.nodeAddress);
            // Add the minipools to the node object
            node.minipools = minipools;
            console.log(`Added ${minipools.length} minipools to node ${node.nodeAddress}`);

            // Now for each minipool we need to get fee structure and balance.
            for (const minipool of node.minipools) {
                const balance = await publicClient.getBalance({address: minipool});
                    
                const minipoolContract = getContract({
                    address: minipool,
                    abi: minipoolDelegateAbi,
                    client: publicClient,
                });

                const operatorFee = Number(await minipoolContract.read.getNodeFee()) / 1e18;
                const depositType = Number(await minipoolContract.read.getDepositType());

                const operatorShare = Number(balance) / depositType;
                const rethShare = depositType == 2 ? Number(balance) * 0.5 : Number(balance) * 0.75;
                const commissionedBalance = operatorShare + (rethShare * operatorFee);
                
                // Add minipool data to the node object
                if (!node.minipoolData) {
                    node.minipoolData = [];
                }
                node.minipoolData.push({
                    address: minipool,
                    operatorShare,
                    rethShare,
                    commissionedBalance
                });

                // Update global totals for the node
                if (!node.totalOperatorShare) node.totalOperatorShare = 0;
                if (!node.totalRethShare) node.totalRethShare = 0;
                if (!node.totalCommissionedBalance) node.totalCommissionedBalance = 0;

                node.totalOperatorShare += operatorShare;
                node.totalRethShare += rethShare;
                node.totalCommissionedBalance += commissionedBalance;

            }

            // Lets get the balance of the fee distributor for the node
            const feeDistributorContract = getContract({address: node.feeDistributor, abi: nodeDistributorDelegateAbi, client: publicClient});
            const feeDistributorBalance = await feeDistributorContract.read.getNodeShare();
            console.log(`Fee distributor balance: ${feeDistributorBalance}`);

            // Add the fee distributor balance to the node object
            node.feeDistributorBalance = bigIntToString(feeDistributorBalance);

            // Lastly, lets get the node's RPL information.
            const rplStakingContract = getContract({
                address: rocketpoolNodeStakingAddress,
                abi: rocketNodeStakingAbi,
                client: publicClient,
            });

            // Get the nodes RPL stake, getNodeEffectiveRPLStake, getNodeMinimumRPLStake, and getNodeETHCollateralisationRatio
            const rplStake = await rplStakingContract.read.getNodeRPLStake([node.nodeAddress]);
            const effectiveRplStake = await rplStakingContract.read.getNodeEffectiveRPLStake([node.nodeAddress]);
            const minimumRplStake = await rplStakingContract.read.getNodeMinimumRPLStake([node.nodeAddress]);
            const ethCollateralisationRatio = await rplStakingContract.read.getNodeETHCollateralisationRatio([node.nodeAddress]);
            const additionalRplNeeded = minimumRplStake - rplStake;

            // Add the RPL information to the node object
            node.rplStake = bigIntToString(rplStake);
            node.effectiveRplStake = bigIntToString(effectiveRplStake);
            node.minimumRplStake = bigIntToString(minimumRplStake);
            node.ethCollateralisationRatio = bigIntToString(ethCollateralisationRatio);
            node.additionalRplNeeded = bigIntToString(additionalRplNeeded);
        }

        // We now want to save all this data to firebase.
        for (const node of nodes) {
            await db.collection("nodes").doc(node.nodeAddress).set(node);
        }

    } catch (error) {
        console.error("Error getting node manager contract address:", error);
    }
})();
