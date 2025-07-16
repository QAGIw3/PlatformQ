const hre = require("hardhat");

async function main() {
  console.log("Deploying PlatformQ smart contracts...");

  // Get the deployer account
  const [deployer] = await hre.ethers.getSigners();
  console.log("Deploying contracts with account:", deployer.address);

  // Deploy SoulBound Token contract
  const SoulBoundToken = await hre.ethers.getContractFactory("SoulBoundToken");
  const sbt = await SoulBoundToken.deploy("PlatformQ SBT", "PQSBT");
  await sbt.deployed();
  console.log("SoulBoundToken deployed to:", sbt.address);

  // Deploy CrossChainBridge contract
  const CrossChainBridge = await hre.ethers.getContractFactory("CrossChainBridge");
  const bridge = await CrossChainBridge.deploy();
  await bridge.deployed();
  console.log("CrossChainBridge deployed to:", bridge.address);

  // Deploy RoyaltyManager contract
  const RoyaltyManager = await hre.ethers.getContractFactory("RoyaltyManager");
  const royaltyManager = await RoyaltyManager.deploy(250); // 2.5% default royalty
  await royaltyManager.deployed();
  console.log("RoyaltyManager deployed to:", royaltyManager.address);

  // Deploy UsageLicense contract
  const UsageLicense = await hre.ethers.getContractFactory("UsageLicense");
  const usageLicense = await UsageLicense.deploy("PlatformQ License", "PQL");
  await usageLicense.deployed();
  console.log("UsageLicense deployed to:", usageLicense.address);

  // Deploy ReputationOracle contract
  const ReputationOracle = await hre.ethers.getContractFactory("ReputationOracle");
  const reputationOracle = await ReputationOracle.deploy();
  await reputationOracle.deployed();
  console.log("ReputationOracle deployed to:", reputationOracle.address);

  // Deploy PlatformQGovernor contract
  const PlatformQGovernor = await hre.ethers.getContractFactory("PlatformQGovernor");
  const governor = await PlatformQGovernor.deploy(
    reputationOracle.address,
    1, // 1 block voting delay
    50400, // ~1 week voting period (assuming 12s blocks)
    1000 // 10% quorum
  );
  await governor.deployed();
  console.log("PlatformQGovernor deployed to:", governor.address);

  // Set up initial oracle operators (in production, these would be different addresses)
  await reputationOracle.addOracle(deployer.address);
  console.log("Added deployer as oracle operator");

  // Save deployment addresses
  const deployments = {
    network: hre.network.name,
    contracts: {
      SoulBoundToken: sbt.address,
      CrossChainBridge: bridge.address,
      RoyaltyManager: royaltyManager.address,
      UsageLicense: usageLicense.address,
      ReputationOracle: reputationOracle.address,
      PlatformQGovernor: governor.address
    },
    deployer: deployer.address,
    timestamp: new Date().toISOString()
  };

  const fs = require("fs");
  fs.writeFileSync(
    `deployments/${hre.network.name}.json`,
    JSON.stringify(deployments, null, 2)
  );
  console.log(`Deployment addresses saved to deployments/${hre.network.name}.json`);

  // Verify contracts on Etherscan (if not local network)
  if (hre.network.name !== "hardhat" && hre.network.name !== "localhost") {
    console.log("Waiting for block confirmations...");
    await sbt.deployTransaction.wait(5);
    
    console.log("Verifying contracts on Etherscan...");
    try {
      await hre.run("verify:verify", {
        address: sbt.address,
        constructorArguments: ["PlatformQ SBT", "PQSBT"],
      });
      
      await hre.run("verify:verify", {
        address: bridge.address,
        constructorArguments: [],
      });
      
      await hre.run("verify:verify", {
        address: royaltyManager.address,
        constructorArguments: [250],
      });
      
      await hre.run("verify:verify", {
        address: usageLicense.address,
        constructorArguments: ["PlatformQ License", "PQL"],
      });
      
      await hre.run("verify:verify", {
        address: reputationOracle.address,
        constructorArguments: [],
      });
      
      await hre.run("verify:verify", {
        address: governor.address,
        constructorArguments: [reputationOracle.address, 1, 50400, 1000],
      });
    } catch (error) {
      console.error("Error verifying contracts:", error);
    }
  }
}

main()
  .then(() => process.exit(0))
  .catch((error) => {
    console.error(error);
    process.exit(1);
  }); 