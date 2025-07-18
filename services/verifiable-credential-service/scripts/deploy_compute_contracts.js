const hre = require("hardhat");
const fs = require("fs");
const path = require("path");

async function main() {
    console.log("🚀 Deploying Compute Market Smart Contracts...");

    const [deployer] = await hre.ethers.getSigners();
    console.log("Deploying contracts with account:", deployer.address);
    console.log("Account balance:", (await deployer.getBalance()).toString());

    // Deploy mock tokens and oracles for development
    let settlementToken, priceOracle, computeRegistry;
    
    if (hre.network.name === "hardhat" || hre.network.name === "localhost") {
        console.log("\n📋 Deploying mock contracts for development...");
        
        // Deploy mock ERC20 token
        const MockERC20 = await hre.ethers.getContractFactory("MockERC20");
        settlementToken = await MockERC20.deploy("USDC Mock", "USDC", 6);
        await settlementToken.deployed();
        console.log("✅ Mock USDC deployed to:", settlementToken.address);
        
        // Deploy mock price oracle
        const MockOracle = await hre.ethers.getContractFactory("MockV3Aggregator");
        priceOracle = await MockOracle.deploy(8, 5000000000); // $50 per GPU hour
        await priceOracle.deployed();
        console.log("✅ Mock Price Oracle deployed to:", priceOracle.address);
        
        // Deploy mock compute registry
        const MockRegistry = await hre.ethers.getContractFactory("MockComputeRegistry");
        computeRegistry = await MockRegistry.deploy();
        await computeRegistry.deployed();
        console.log("✅ Mock Compute Registry deployed to:", computeRegistry.address);
    } else {
        // Use existing contracts on testnet/mainnet
        settlementToken = { address: process.env.SETTLEMENT_TOKEN_ADDRESS };
        priceOracle = { address: process.env.PRICE_ORACLE_ADDRESS };
        computeRegistry = { address: process.env.COMPUTE_REGISTRY_ADDRESS };
    }

    // 1. Deploy ComputeFutures
    console.log("\n📋 Deploying ComputeFutures...");
    const ComputeFutures = await hre.ethers.getContractFactory("ComputeFutures");
    const computeFutures = await hre.upgrades.deployProxy(
        ComputeFutures,
        [settlementToken.address, computeRegistry.address, priceOracle.address],
        { initializer: "initialize" }
    );
    await computeFutures.deployed();
    console.log("✅ ComputeFutures deployed to:", computeFutures.address);

    // 2. Deploy ComputeOptions
    console.log("\n📋 Deploying ComputeOptions...");
    const ComputeOptions = await hre.ethers.getContractFactory("ComputeOptions");
    const computeOptions = await hre.upgrades.deployProxy(
        ComputeOptions,
        [settlementToken.address, computeFutures.address, priceOracle.address],
        { initializer: "initialize" }
    );
    await computeOptions.deployed();
    console.log("✅ ComputeOptions deployed to:", computeOptions.address);

    // 3. Deploy ComputeStablecoin
    console.log("\n📋 Deploying ComputeStablecoin...");
    const ComputeStablecoin = await hre.ethers.getContractFactory("ComputeStablecoin");
    const computeStablecoin = await hre.upgrades.deployProxy(
        ComputeStablecoin,
        [],
        { initializer: "initialize" }
    );
    await computeStablecoin.deployed();
    console.log("✅ ComputeStablecoin deployed to:", computeStablecoin.address);

    // 4. Deploy ComputeOrderBook
    console.log("\n📋 Deploying ComputeOrderBook...");
    const ComputeOrderBook = await hre.ethers.getContractFactory("ComputeOrderBook");
    const computeOrderBook = await hre.upgrades.deployProxy(
        ComputeOrderBook,
        [settlementToken.address, computeRegistry.address, priceOracle.address],
        { initializer: "initialize" }
    );
    await computeOrderBook.deployed();
    console.log("✅ ComputeOrderBook deployed to:", computeOrderBook.address);

    // 5. Deploy ComputeMarketBridge (if LayerZero endpoint is available)
    let computeMarketBridge;
    if (process.env.LAYERZERO_ENDPOINT) {
        console.log("\n📋 Deploying ComputeMarketBridge...");
        const ComputeMarketBridge = await hre.ethers.getContractFactory("ComputeMarketBridge");
        computeMarketBridge = await hre.upgrades.deployProxy(
            ComputeMarketBridge,
            [process.env.LAYERZERO_ENDPOINT],
            { initializer: "initialize" }
        );
        await computeMarketBridge.deployed();
        console.log("✅ ComputeMarketBridge deployed to:", computeMarketBridge.address);
    } else {
        console.log("⚠️  Skipping ComputeMarketBridge deployment (no LayerZero endpoint)");
    }

    // Configure contracts
    console.log("\n🔧 Configuring contracts...");

    // Grant roles
    const OPERATOR_ROLE = await computeFutures.OPERATOR_ROLE();
    await computeFutures.grantRole(OPERATOR_ROLE, deployer.address);
    await computeOptions.grantRole(OPERATOR_ROLE, deployer.address);
    await computeStablecoin.grantRole(OPERATOR_ROLE, deployer.address);
    await computeOrderBook.grantRole(OPERATOR_ROLE, deployer.address);
    console.log("✅ Granted operator roles");

    // Deploy stablecoins
    console.log("\n💰 Deploying stablecoins...");
    
    // Deploy cFLOPS
    await computeStablecoin.deployStablecoin(
        0, // CFLOPS
        1, // COLLATERALIZED mode
        "Compute FLOPS",
        "cFLOPS",
        hre.ethers.utils.parseEther("0.05"), // $0.05 per TFLOPS
        priceOracle.address
    );
    console.log("✅ cFLOPS deployed");

    // Deploy cGPUH
    await computeStablecoin.deployStablecoin(
        1, // CGPUH
        2, // HYBRID mode
        "Compute GPU Hours",
        "cGPUH",
        hre.ethers.utils.parseEther("50"), // $50 per GPU hour
        priceOracle.address
    );
    console.log("✅ cGPUH deployed");

    // Add collateral assets
    await computeStablecoin.addCollateralAsset(
        settlementToken.address,
        priceOracle.address,
        8000 // 80% collateral factor
    );
    console.log("✅ Added USDC as collateral");

    // Configure cross-chain bridge (if deployed)
    if (computeMarketBridge) {
        console.log("\n🌉 Configuring cross-chain bridge...");
        
        // Configure for Polygon
        if (hre.network.name === "polygon" || hre.network.name === "mumbai") {
            await computeMarketBridge.configureChain(
                137, // Polygon chain ID
                109, // LayerZero Polygon ID
                computeFutures.address,
                computeOptions.address,
                settlementToken.address,
                200000 // Gas limit
            );
            console.log("✅ Configured Polygon chain");
        }
    }

    // Create initial markets
    console.log("\n📊 Creating initial markets...");
    
    // Create day-ahead market for tomorrow
    const tomorrow = Math.floor(Date.now() / 1000) + 86400;
    const deliveryHour = Math.floor(tomorrow / 3600) * 3600; // Round to hour
    
    await computeFutures.createDayAheadMarket(deliveryHour, 0); // GPU market
    console.log("✅ Created GPU day-ahead market");

    // Save deployment addresses
    const deployments = {
        network: hre.network.name,
        chainId: hre.network.config.chainId,
        contracts: {
            ComputeFutures: {
                address: computeFutures.address,
                implementation: await hre.upgrades.erc1967.getImplementationAddress(computeFutures.address)
            },
            ComputeOptions: {
                address: computeOptions.address,
                implementation: await hre.upgrades.erc1967.getImplementationAddress(computeOptions.address)
            },
            ComputeStablecoin: {
                address: computeStablecoin.address,
                implementation: await hre.upgrades.erc1967.getImplementationAddress(computeStablecoin.address)
            },
            ComputeOrderBook: {
                address: computeOrderBook.address,
                implementation: await hre.upgrades.erc1967.getImplementationAddress(computeOrderBook.address)
            }
        },
        supportContracts: {
            settlementToken: settlementToken.address,
            priceOracle: priceOracle.address,
            computeRegistry: computeRegistry.address
        },
        deployer: deployer.address,
        timestamp: new Date().toISOString()
    };

    if (computeMarketBridge) {
        deployments.contracts.ComputeMarketBridge = {
            address: computeMarketBridge.address,
            implementation: await hre.upgrades.erc1967.getImplementationAddress(computeMarketBridge.address)
        };
    }

    const deploymentsPath = path.join(__dirname, `../deployments/compute-${hre.network.name}.json`);
    fs.mkdirSync(path.dirname(deploymentsPath), { recursive: true });
    fs.writeFileSync(deploymentsPath, JSON.stringify(deployments, null, 2));
    console.log(`\n💾 Deployment addresses saved to ${deploymentsPath}`);

    // Verify contracts on Etherscan (if not local)
    if (hre.network.name !== "hardhat" && hre.network.name !== "localhost") {
        console.log("\n🔍 Waiting for block confirmations before verification...");
        await computeFutures.deployTransaction.wait(5);
        
        console.log("Verifying contracts on Etherscan...");
        
        try {
            // Verify implementation contracts
            await hre.run("verify:verify", {
                address: await hre.upgrades.erc1967.getImplementationAddress(computeFutures.address),
                constructorArguments: [],
            });
            
            await hre.run("verify:verify", {
                address: await hre.upgrades.erc1967.getImplementationAddress(computeOptions.address),
                constructorArguments: [],
            });
            
            await hre.run("verify:verify", {
                address: await hre.upgrades.erc1967.getImplementationAddress(computeStablecoin.address),
                constructorArguments: [],
            });
            
            await hre.run("verify:verify", {
                address: await hre.upgrades.erc1967.getImplementationAddress(computeOrderBook.address),
                constructorArguments: [],
            });
            
            console.log("✅ Contracts verified on Etherscan");
        } catch (error) {
            console.error("❌ Error verifying contracts:", error);
        }
    }

    console.log("\n🎉 Compute Market deployment complete!");
    console.log("\n📋 Summary:");
    console.log(`  ComputeFutures: ${computeFutures.address}`);
    console.log(`  ComputeOptions: ${computeOptions.address}`);
    console.log(`  ComputeStablecoin: ${computeStablecoin.address}`);
    console.log(`  ComputeOrderBook: ${computeOrderBook.address}`);
    if (computeMarketBridge) {
        console.log(`  ComputeMarketBridge: ${computeMarketBridge.address}`);
    }
}

// Execute deployment
main()
    .then(() => process.exit(0))
    .catch((error) => {
        console.error(error);
        process.exit(1);
    }); 