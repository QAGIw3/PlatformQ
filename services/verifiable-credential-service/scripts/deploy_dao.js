const hre = require("hardhat");
const fs = require("fs");
const path =require("path");

async function main() {
    console.log("Deploying DAO contracts...");

    const [deployer] = await hre.ethers.getSigners();
    console.log("Deploying contracts with the account:", deployer.address);

    // Get the contract factories
    const ReputationOracle = await hre.ethers.getContractFactory("ReputationOracle");
    const Treasury = await hre.ethers.getContractFactory("Treasury");
    const PlatformQGovernor = await hre.ethers.getContractFactory("PlatformQGovernor");
    const TimelockController = await hre.ethers.getContractFactory("TimelockController");

    // 1. Deploy ReputationOracle
    console.log("Deploying ReputationOracle...");
    const reputationOracle = await ReputationOracle.deploy();
    await reputationOracle.deployed();
    console.log("ReputationOracle deployed to:", reputationOracle.address);
    
    // 1.1. Grant the deployer the ability to set reputation initially.
    // In a real-world scenario, this would be a multi-sig or a dedicated service account.
    console.log("Setting deployer as an initial reputation setter...");
    await reputationOracle.transferOwnership(deployer.address);
    console.log(`Ownership of ReputationOracle transferred to deployer: ${deployer.address}`);

    // 2. Deploy TimelockController
    console.log("Deploying TimelockController...");
    const minDelay = 1; // 1 second for local dev, adjust for production
    const proposers = [deployer.address];
    const executors = [deployer.address];
    const admin = deployer.address;
    const timelock = await TimelockController.deploy(minDelay, proposers, executors, admin);
    await timelock.deployed();
    console.log("TimelockController deployed to:", timelock.address);

    // 3. Deploy PlatformQGovernor
    console.log("Deploying PlatformQGovernor...");
    const votingDelay = 1; // 1 block
    const votingPeriod = 50400; // 1 week in blocks (assuming 12s block time)
    const proposalThreshold = 0;
    const initialQuorum = 0; // No minimum quorum for testing
    const governor = await PlatformQGovernor.deploy(
        "PlatformQ DAO",
        reputationOracle.address,
        votingDelay,
        votingPeriod,
        proposalThreshold,
        initialQuorum
    );
    await governor.deployed();
    console.log("PlatformQGovernor deployed to:", governor.address);
    
    // 4. Deploy Treasury
    console.log("Deploying Treasury...");
    const treasury = await Treasury.deploy();
    await treasury.deployed();
    console.log("Treasury deployed to:", treasury.address);

    // 5. Transfer ownership of Treasury to the Governor
    console.log("Transferring ownership of Treasury to Governor...");
    await treasury.transferOwnership(governor.address);
    console.log("Ownership of Treasury transferred to:", governor.address);

    // 6. Setup roles in TimelockController
    console.log("Setting up roles in TimelockController...");
    const proposerRole = await timelock.PROPOSER_ROLE();
    const executorRole = await timelock.EXECUTOR_ROLE();
    const adminRole = await timelock.TIMELOCK_ADMIN_ROLE();

    await timelock.grantRole(proposerRole, governor.address);
    console.log(`Governor (${governor.address}) granted PROPOSER_ROLE`);

    // The deployer should renounce the admin role to make the timelock trustless
    await timelock.renounceRole(adminRole, deployer.address);
    console.log(`Deployer (${deployer.address}) renounced TIMELOCK_ADMIN_ROLE`);
    
    // Verify contracts on Etherscan
    if (hre.network.name !== "hardhat" && hre.network.name !== "localhost") {
        console.log("Waiting for block confirmations...");
        await reputationOracle.deployTransaction.wait(5);
        await treasury.deployTransaction.wait(5);
        await governor.deployTransaction.wait(5);
        await timelock.deployTransaction.wait(5);

        console.log("Verifying contracts on Etherscan...");
        // Add verification logic here...
    }

    // Save deployment info
    const deploymentInfo = {
        network: hre.network.name,
        reputationOracle: reputationOracle.address,
        treasury: treasury.address,
        governor: governor.address,
        timelock: timelock.address
    };

    const deploymentsDir = path.join(__dirname, "..", "deployments");
    if (!fs.existsSync(deploymentsDir)) {
        fs.mkdirSync(deploymentsDir);
    }
    fs.writeFileSync(
        path.join(deploymentsDir, `dao_${hre.network.name}.json`),
        JSON.stringify(deploymentInfo, null, 2)
    );
    console.log(`\nDeployment info saved to deployments/dao_${hre.network.name}.json`);
}

main()
    .then(() => process.exit(0))
    .catch((error) => {
        console.error(error);
        process.exit(1);
    }); 