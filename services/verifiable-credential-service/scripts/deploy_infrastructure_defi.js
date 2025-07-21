const hre = require("hardhat");
const { ethers } = require("hardhat");

async function main() {
  console.log("Deploying Infrastructure DeFi contracts...");

  // Get the deployer account
  const [deployer] = await hre.ethers.getSigners();
  console.log("Deploying contracts with account:", deployer.address);
  console.log("Account balance:", (await deployer.getBalance()).toString());

  // Deploy ResourceToken contract
  console.log("\n1. Deploying ResourceToken...");
  const ResourceToken = await hre.ethers.getContractFactory("ResourceToken");
  const resourceToken = await ResourceToken.deploy("https://api.platformq.io/metadata/");
  await resourceToken.deployed();
  console.log("ResourceToken deployed to:", resourceToken.address);

  // Deploy ResourceAMM contract
  console.log("\n2. Deploying ResourceAMM...");
  const ResourceAMM = await hre.ethers.getContractFactory("ResourceAMM");
  const resourceAMM = await ResourceAMM.deploy(resourceToken.address);
  await resourceAMM.deployed();
  console.log("ResourceAMM deployed to:", resourceAMM.address);

  // Deploy InfrastructureLending contract
  console.log("\n3. Deploying InfrastructureLending...");
  const InfrastructureLending = await hre.ethers.getContractFactory("InfrastructureLending");
  const infrastructureLending = await InfrastructureLending.deploy(deployer.address); // Using deployer as oracle initially
  await infrastructureLending.deployed();
  console.log("InfrastructureLending deployed to:", infrastructureLending.address);

  // Deploy mock USDC for testing
  console.log("\n4. Deploying mock USDC...");
  const MockERC20 = await hre.ethers.getContractFactory("contracts/mocks/MockERC20.sol:MockERC20");
  const usdc = await MockERC20.deploy("USD Coin", "USDC", 6);
  await usdc.deployed();
  console.log("Mock USDC deployed to:", usdc.address);

  // Setup roles
  console.log("\n5. Setting up roles...");
  const MINTER_ROLE = await resourceToken.MINTER_ROLE();
  const BURNER_ROLE = await resourceToken.BURNER_ROLE();
  const SLASHER_ROLE = await resourceToken.SLASHER_ROLE();
  const ORACLE_ROLE = await resourceToken.ORACLE_ROLE();

  // Grant roles to settlement coordinator (would be the actual address in production)
  const settlementCoordinator = deployer.address; // Placeholder
  await resourceToken.grantRole(MINTER_ROLE, settlementCoordinator);
  await resourceToken.grantRole(BURNER_ROLE, settlementCoordinator);
  await resourceToken.grantRole(SLASHER_ROLE, settlementCoordinator);
  await resourceToken.grantRole(ORACLE_ROLE, deployer.address);
  console.log("Roles granted to settlement coordinator");

  // Register some test providers
  console.log("\n6. Registering test providers...");
  const providers = [
    "0x70997970C51812dc3A010C7d01b50e0d17dc79C8",
    "0x3C44CdDdB6a900fa2b585dd299e03d12FA4293BC",
    "0x90F79bf6EB2c4f870365E785982E1f101E93b906"
  ];

  for (const provider of providers) {
    await resourceToken.registerProvider(provider, 500); // 500 reputation
    console.log(`Registered provider: ${provider}`);
    
    // Set some capacity
    await resourceToken.setProviderCapacity(provider, 0, 1000); // CPU
    await resourceToken.setProviderCapacity(provider, 1, 100);  // GPU
    await resourceToken.setProviderCapacity(provider, 2, 10000); // Storage
  }

  // Create AMM pools
  console.log("\n7. Creating AMM pools...");
  
  // CPU/USDC pool
  console.log("Creating CPU/USDC pool...");
  const cpuPoolTx = await resourceAMM.createPool(
    1, // Assuming token ID 1 for CPU
    usdc.address,
    30 // 0.3% fee
  );
  await cpuPoolTx.wait();
  
  // GPU/USDC pool
  console.log("Creating GPU/USDC pool...");
  const gpuPoolTx = await resourceAMM.createPool(
    2, // Assuming token ID 2 for GPU
    usdc.address,
    50 // 0.5% fee (higher for more volatile asset)
  );
  await gpuPoolTx.wait();

  // Storage/USDC pool
  console.log("Creating Storage/USDC pool...");
  const storagePoolTx = await resourceAMM.createPool(
    3, // Assuming token ID 3 for Storage
    usdc.address,
    10 // 0.1% fee (lower for stable asset)
  );
  await storagePoolTx.wait();

  // Save deployment addresses
  const deployments = {
    network: hre.network.name,
    deployedAt: new Date().toISOString(),
    contracts: {
      ResourceToken: resourceToken.address,
      ResourceAMM: resourceAMM.address,
      InfrastructureLending: infrastructureLending.address,
      MockUSDC: usdc.address
    },
    providers: providers,
    pools: {
      "CPU/USDC": { poolId: 0, fee: "0.3%" },
      "GPU/USDC": { poolId: 1, fee: "0.5%" },
      "Storage/USDC": { poolId: 2, fee: "0.1%" }
    }
  };

  console.log("\n=== Deployment Summary ===");
  console.log(JSON.stringify(deployments, null, 2));

  // Save to file
  const fs = require("fs");
  const path = require("path");
  const deploymentsDir = path.join(__dirname, "../deployments");
  
  if (!fs.existsSync(deploymentsDir)) {
    fs.mkdirSync(deploymentsDir, { recursive: true });
  }
  
  fs.writeFileSync(
    path.join(deploymentsDir, `${hre.network.name}.json`),
    JSON.stringify(deployments, null, 2)
  );

  console.log("\nDeployment complete! Addresses saved to deployments/", hre.network.name + ".json");

  // Verify contracts on Etherscan (if not on localhost)
  if (hre.network.name !== "localhost" && hre.network.name !== "hardhat") {
    console.log("\nVerifying contracts on Etherscan...");
    
    await hre.run("verify:verify", {
      address: resourceToken.address,
      constructorArguments: ["https://api.platformq.io/metadata/"],
    });

    await hre.run("verify:verify", {
      address: resourceAMM.address,
      constructorArguments: [resourceToken.address],
    });

    await hre.run("verify:verify", {
      address: infrastructureLending.address,
      constructorArguments: [deployer.address],
    });
  }
}

main()
  .then(() => process.exit(0))
  .catch((error) => {
    console.error(error);
    process.exit(1);
  }); 