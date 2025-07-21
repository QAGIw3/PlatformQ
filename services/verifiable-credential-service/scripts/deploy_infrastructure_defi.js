const hre = require("hardhat");
const { ethers } = require("hardhat");

async function main() {
  console.log("Deploying Infrastructure DeFi contracts...");

  // Get the deployer account
  const [deployer] = await hre.ethers.getSigners();
  console.log("Deploying Infrastructure DeFi contracts with account:", deployer.address);
  console.log("Account balance:", (await deployer.getBalance()).toString());

  // Deploy ResourceToken
  const ResourceToken = await hre.ethers.getContractFactory("ResourceToken");
  const resourceToken = await ResourceToken.deploy();
  await resourceToken.deployed();
  console.log("ResourceToken deployed to:", resourceToken.address);

  // Deploy ResourceAMM
  const ResourceAMM = await hre.ethers.getContractFactory("ResourceAMM");
  const resourceAMM = await ResourceAMM.deploy(resourceToken.address);
  await resourceAMM.deployed();
  console.log("ResourceAMM deployed to:", resourceAMM.address);

  // Deploy InfrastructureLending
  const InfrastructureLending = await hre.ethers.getContractFactory("InfrastructureLending");
  const infrastructureLending = await InfrastructureLending.deploy(
      resourceToken.address,
      "0x0000000000000000000000000000000000000000" // Update with real stablecoin
  );
  await infrastructureLending.deployed();
  console.log("InfrastructureLending deployed to:", infrastructureLending.address);

  // Deploy InfrastructureRewards
  const InfrastructureRewards = await hre.ethers.getContractFactory("InfrastructureRewards");
  const infrastructureRewards = await InfrastructureRewards.deploy(
      resourceToken.address,
      resourceAMM.address,
      infrastructureLending.address
  );
  await infrastructureRewards.deployed();
  console.log("InfrastructureRewards deployed to:", infrastructureRewards.address);

  // Deploy FlashResourceProvider
  const FlashResourceProvider = await hre.ethers.getContractFactory("FlashResourceProvider");
  const flashResourceProvider = await FlashResourceProvider.deploy(
      resourceToken.address,
      resourceAMM.address
  );
  await flashResourceProvider.deployed();
  console.log("FlashResourceProvider deployed to:", flashResourceProvider.address);

  // Grant roles
  const MINTER_ROLE = await resourceToken.MINTER_ROLE();
  const OPERATOR_ROLE = await resourceToken.OPERATOR_ROLE();

  // Grant roles to AMM
  await resourceToken.grantRole(MINTER_ROLE, resourceAMM.address);
  await resourceToken.grantRole(OPERATOR_ROLE, resourceAMM.address);
  console.log("Granted AMM roles");

  // Grant roles to lending
  await resourceToken.grantRole(OPERATOR_ROLE, infrastructureLending.address);
  console.log("Granted lending roles");

  // Grant roles to rewards
  await resourceToken.grantRole(MINTER_ROLE, infrastructureRewards.address);
  console.log("Granted rewards roles");

  // Grant operator role to flash provider
  await resourceToken.grantRole(OPERATOR_ROLE, flashResourceProvider.address);
  console.log("Granted flash provider roles");

  // Deploy ResourceStaking contract
  console.log("\nDeploying ResourceStaking contract...");
  const ResourceStaking = await hre.ethers.getContractFactory("ResourceStaking");
  const resourceStaking = await ResourceStaking.deploy(
    resourceToken.address,
    infrastructureRewards.address
  );
  await resourceStaking.deployed();
  console.log("ResourceStaking deployed to:", resourceStaking.address);

  // Grant roles for ResourceStaking
  const STAKING_OPERATOR_ROLE = await resourceStaking.OPERATOR_ROLE();
  const SLASHER_ROLE = await resourceStaking.SLASHER_ROLE();
  const REWARD_MANAGER_ROLE = await resourceStaking.REWARD_MANAGER_ROLE();
  
  await resourceStaking.grantRole(STAKING_OPERATOR_ROLE, deployer.address);
  await resourceStaking.grantRole(SLASHER_ROLE, deployer.address);
  await resourceStaking.grantRole(REWARD_MANAGER_ROLE, deployer.address);
  await resourceToken.grantRole(OPERATOR_ROLE, resourceStaking.address);
  console.log("Staking roles assigned");

  // Deploy InfrastructureVault (example for CPU resources)
  console.log("\nDeploying InfrastructureVault for CPU...");
  const InfrastructureVault = await hre.ethers.getContractFactory("InfrastructureVault");
  const cpuVault = await InfrastructureVault.deploy(
    resourceToken.address,
    0, // CPU resource token ID
    resourceAMM.address,
    infrastructureLending.address,
    flashResourceProvider.address,
    resourceStaking.address,
    "CPU Vault Token",
    "vCPU"
  );
  await cpuVault.deployed();
  console.log("CPU Vault deployed to:", cpuVault.address);

  // Grant roles for Vault
  const STRATEGIST_ROLE = await cpuVault.STRATEGIST_ROLE();
  const KEEPER_ROLE = await cpuVault.KEEPER_ROLE();
  const GUARDIAN_ROLE = await cpuVault.GUARDIAN_ROLE();
  
  await cpuVault.grantRole(STRATEGIST_ROLE, deployer.address);
  await cpuVault.grantRole(KEEPER_ROLE, deployer.address);
  await cpuVault.grantRole(GUARDIAN_ROLE, deployer.address);
  await resourceToken.grantRole(OPERATOR_ROLE, cpuVault.address);
  console.log("Vault roles assigned");

  // Create initial AMM pools
  const cpuPrice = hre.ethers.utils.parseEther("0.05");
  const gpuPrice = hre.ethers.utils.parseEther("0.50");
  const storagePrice = hre.ethers.utils.parseEther("0.001");

  await resourceAMM.createPool(0, cpuPrice); // CPU pool
  await resourceAMM.createPool(1, gpuPrice); // GPU pool
  await resourceAMM.createPool(2, storagePrice); // Storage pool
  console.log("Created initial AMM pools");

  // Save deployment addresses
  const deploymentInfo = {
      network: hre.network.name,
      deployer: deployer.address,
      contracts: {
          ResourceToken: resourceToken.address,
          ResourceAMM: resourceAMM.address,
          InfrastructureLending: infrastructureLending.address,
          InfrastructureRewards: infrastructureRewards.address,
          FlashResourceProvider: flashResourceProvider.address,
          ResourceStaking: resourceStaking.address,
          CPUVault: cpuVault.address
      },
      pools: {
          CPU: 0,
          GPU: 1,
          Storage: 2
      }
  };

  console.log("\nDeployment completed!");
  console.log(JSON.stringify(deploymentInfo, null, 2));

  // Write to file for other services
  const fs = require('fs');
  fs.writeFileSync(
      `deployments/${hre.network.name}-infrastructure-defi.json`,
      JSON.stringify(deploymentInfo, null, 2)
  );
}

main()
    .then(() => process.exit(0))
    .catch((error) => {
        console.error(error);
        process.exit(1);
    }); 