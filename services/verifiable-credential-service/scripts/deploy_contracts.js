const hre = require("hardhat");

async function main() {
  // Get the contract factories
  const CredentialRegistry = await hre.ethers.getContractFactory("CredentialRegistry");
  const CredentialVerifier = await hre.ethers.getContractFactory("CredentialVerifier");
  const GnosisSafe = await hre.ethers.getContractFactory("GnosisSafe");
  const ReputationOracle = await hre.ethers.getContractFactory("ReputationOracle");

  console.log("Deploying contracts...");

  // Deploy CredentialRegistry
  console.log("Deploying CredentialRegistry...");
  const registry = await CredentialRegistry.deploy();
  await registry.deployed();
  console.log("CredentialRegistry deployed to:", registry.address);

  // Deploy CredentialVerifier with registry address
  console.log("Deploying CredentialVerifier...");
  const verifier = await CredentialVerifier.deploy(registry.address);
  await verifier.deployed();
  console.log("CredentialVerifier deployed to:", verifier.address);

  // Deploy GnosisSafe
  console.log("Deploying GnosisSafe...");
  const gnosisSafe = await GnosisSafe.deploy();
  await gnosisSafe.deployed();
  console.log("GnosisSafe deployed to:", gnosisSafe.address);

  // Deploy ReputationOracle
  console.log("Deploying ReputationOracle...");
  const reputationOracle = await ReputationOracle.deploy();
  await reputationOracle.deployed();
  console.log("ReputationOracle deployed to:", reputationOracle.address);

  // Verify contracts on Etherscan (if not on local network)
  if (hre.network.name !== "hardhat" && hre.network.name !== "localhost") {
    console.log("Waiting for block confirmations...");
    await registry.deployTransaction.wait(5);
    await verifier.deployTransaction.wait(5);
    await gnosisSafe.deployTransaction.wait(5);
    await reputationOracle.deployTransaction.wait(5);

    console.log("Verifying contracts on Etherscan...");
    try {
      await hre.run("verify:verify", {
        address: registry.address,
        constructorArguments: [],
      });

      await hre.run("verify:verify", {
        address: verifier.address,
        constructorArguments: [registry.address],
      });
      
      await hre.run("verify:verify", {
        address: gnosisSafe.address,
        constructorArguments: [],
      });

      await hre.run("verify:verify", {
        address: reputationOracle.address,
        constructorArguments: [],
      });

    } catch (error) {
      console.error("Error verifying contracts:", error);
    }
  }

  // Save deployment info
  const deploymentInfo = {
    network: hre.network.name,
    registry: {
      address: registry.address,
      transactionHash: registry.deployTransaction.hash,
    },
    verifier: {
      address: verifier.address,
      transactionHash: verifier.deployTransaction.hash,
    },
    gnosisSafe: {
      address: gnosisSafe.address,
      transactionHash: gnosisSafe.deployTransaction.hash,
    },
    reputationOracle: {
      address: reputationOracle.address,
      transactionHash: reputationOracle.deployTransaction.hash,
    },
    deployedAt: new Date().toISOString(),
  };

  console.log("\nDeployment Summary:");
  console.log(JSON.stringify(deploymentInfo, null, 2));

  // Write deployment info to file
  const fs = require("fs");
  const path = require("path");
  const deploymentsDir = path.join(__dirname, "..", "deployments");
  
  if (!fs.existsSync(deploymentsDir)) {
    fs.mkdirSync(deploymentsDir);
  }

  fs.writeFileSync(
    path.join(deploymentsDir, `${hre.network.name}.json`),
    JSON.stringify(deploymentInfo, null, 2)
  );

  console.log(`\nDeployment info saved to deployments/${hre.network.name}.json`);
}

main()
  .then(() => process.exit(0))
  .catch((error) => {
    console.error(error);
    process.exit(1);
  }); 