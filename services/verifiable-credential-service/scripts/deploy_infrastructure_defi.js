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

  // Deploy Derivatives Contracts
  console.log("\nDeploying Derivatives contracts...");
  
  // Use the deployed stablecoin address or deploy a mock one
  const settlementToken = "0x0000000000000000000000000000000000000000"; // Update with real USDC
  
  // Deploy ResourceOptions
  const ResourceOptions = await hre.ethers.getContractFactory("ResourceOptions");
  const resourceOptions = await ResourceOptions.deploy(
    resourceToken.address,
    resourceAMM.address,
    settlementToken
  );
  await resourceOptions.deployed();
  console.log("ResourceOptions deployed to:", resourceOptions.address);

  // Deploy ResourcePerpetuals
  const ResourcePerpetuals = await hre.ethers.getContractFactory("ResourcePerpetuals");
  const resourcePerpetuals = await ResourcePerpetuals.deploy(
    resourceToken.address,
    resourceAMM.address,
    settlementToken
  );
  await resourcePerpetuals.deployed();
  console.log("ResourcePerpetuals deployed to:", resourcePerpetuals.address);

  // Deploy OptionsAMM
  const OptionsAMM = await hre.ethers.getContractFactory("OptionsAMM");
  const optionsAMM = await OptionsAMM.deploy(
    resourceToken.address,
    resourceOptions.address,
    settlementToken
  );
  await optionsAMM.deployed();
  console.log("OptionsAMM deployed to:", optionsAMM.address);

  // Grant roles for derivatives
  const MARKET_MAKER_ROLE = await resourceOptions.MARKET_MAKER_ROLE();
  const ORACLE_ROLE = await resourceOptions.ORACLE_ROLE();
  const PERPETUALS_KEEPER_ROLE = await resourcePerpetuals.KEEPER_ROLE();
  const PERPETUALS_ORACLE_ROLE = await resourcePerpetuals.ORACLE_ROLE();
  const LIQUIDATOR_ROLE = await resourcePerpetuals.LIQUIDATOR_ROLE();
  const AMM_LP_ROLE = await optionsAMM.LP_ROLE();
  const AMM_KEEPER_ROLE = await optionsAMM.KEEPER_ROLE();

  // Grant options roles
  await resourceOptions.grantRole(MARKET_MAKER_ROLE, optionsAMM.address);
  await resourceOptions.grantRole(ORACLE_ROLE, deployer.address);
  console.log("Options roles assigned");

  // Grant perpetuals roles
  await resourcePerpetuals.grantRole(PERPETUALS_KEEPER_ROLE, deployer.address);
  await resourcePerpetuals.grantRole(PERPETUALS_ORACLE_ROLE, deployer.address);
  await resourcePerpetuals.grantRole(LIQUIDATOR_ROLE, deployer.address);
  console.log("Perpetuals roles assigned");

  // Grant AMM roles
  await optionsAMM.grantRole(AMM_LP_ROLE, deployer.address);
  await optionsAMM.grantRole(AMM_KEEPER_ROLE, deployer.address);
  await optionsAMM.grantRole(MARKET_MAKER_ROLE, deployer.address);
  console.log("Options AMM roles assigned");

  // Grant resource token operator roles
  await resourceToken.grantRole(OPERATOR_ROLE, resourceOptions.address);
  await resourceToken.grantRole(OPERATOR_ROLE, optionsAMM.address);
  console.log("Resource token roles for derivatives assigned");

  // Create perpetual markets
  const maxOpenInterest = hre.ethers.utils.parseEther("1000000");
  await resourcePerpetuals.createMarket(0, maxOpenInterest); // CPU perpetuals
  await resourcePerpetuals.createMarket(1, maxOpenInterest); // GPU perpetuals
  await resourcePerpetuals.createMarket(2, maxOpenInterest); // Storage perpetuals
  console.log("Created perpetual markets");

  // Set initial oracle prices for options
  await resourceOptions.updateSpotPrice(0, cpuPrice);
  await resourceOptions.updateSpotPrice(1, gpuPrice);
  await resourceOptions.updateSpotPrice(2, storagePrice);
  
  // Set initial implied volatility (50% as an example)
  const initialIV = 5000; // 50% in basis points
  await resourceOptions.updateImpliedVolatility(0, initialIV);
  await resourceOptions.updateImpliedVolatility(1, initialIV);
  await resourceOptions.updateImpliedVolatility(2, initialIV);
  console.log("Set initial options oracle data");

  // Set initial perpetuals oracle prices
  await resourcePerpetuals.updatePrices(0, cpuPrice, cpuPrice);
  await resourcePerpetuals.updatePrices(1, gpuPrice, gpuPrice);
  await resourcePerpetuals.updatePrices(2, storagePrice, storagePrice);
  console.log("Set initial perpetuals oracle data");

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
          CPUVault: cpuVault.address,
          ResourceOptions: resourceOptions.address,
          ResourcePerpetuals: resourcePerpetuals.address,
          OptionsAMM: optionsAMM.address
      },
      pools: {
          CPU: 0,
          GPU: 1,
          Storage: 2
      },
      derivatives: {
          perpetualMarkets: {
              CPU: 0,
              GPU: 1,
              Storage: 2
          },
          settlementToken: settlementToken
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