const { expect } = require("chai");
const { ethers } = require("hardhat");

describe("PlatformQ DAO", function () {
    let ReputationOracle, reputationOracle;
    let Treasury, treasury;
    let PlatformQGovernor, governor;
    let TimelockController, timelock;
    let owner, addr1, addr2;

    beforeEach(async function () {
        [owner, addr1, addr2] = await ethers.getSigners();

        // Deploy ReputationOracle
        ReputationOracle = await ethers.getContractFactory("ReputationOracle");
        reputationOracle = await ReputationOracle.deploy();
        await reputationOracle.deployed();

        // Deploy TimelockController
        TimelockController = await ethers.getContractFactory("TimelockController");
        const minDelay = 1; // 1 second
        const proposers = [owner.address];
        const executors = [owner.address];
        timelock = await TimelockController.deploy(minDelay, proposers, executors, owner.address);
        await timelock.deployed();

        // Deploy PlatformQGovernor
        PlatformQGovernor = await ethers.getContractFactory("PlatformQGovernor");
        const votingDelay = 1; // 1 block
        const votingPeriod = 10; // 10 blocks for testing
        const proposalThreshold = 0;
        const initialQuorum = 0; // For testing
        governor = await PlatformQGovernor.deploy(
            "PlatformQ DAO",
            reputationOracle.address,
            votingDelay,
            votingPeriod,
            proposalThreshold,
            initialQuorum
        );
        await governor.deployed();

        // Deploy Treasury
        Treasury = await ethers.getContractFactory("Treasury");
        treasury = await Treasury.deploy();
        await treasury.deployed();

        // Transfer ownership of Treasury to the Governor
        await treasury.transferOwnership(governor.address);

        // Setup roles
        const proposerRole = await timelock.PROPOSER_ROLE();
        await timelock.grantRole(proposerRole, governor.address);
    });

    it("Should set the correct owner for the Treasury", async function () {
        expect(await treasury.owner()).to.equal(governor.address);
    });

    it("Should allow a user with reputation to create a proposal and vote", async function () {
        // Grant reputation to addr1
        await reputationOracle.setReputation(addr1.address, 100);
        expect(await reputationOracle.getReputation(addr1.address)).to.equal(100);

        // Create a proposal
        const proposalDescription = "Test Proposal";
        const transferCalldata = treasury.interface.encodeFunctionData("executeTransaction", [addr2.address, ethers.utils.parseEther("1"), "0x"]);
        
        // Have to connect as addr1 to propose
        const tx = await governor.connect(addr1).propose(
            [treasury.address],
            [0],
            [transferCalldata],
            proposalDescription
        );
        const receipt = await tx.wait();
        const proposalId = receipt.events.find(e => e.event === 'ProposalCreated').args.proposalId;

        // Wait for voting delay
        await hre.network.provider.send("evm_mine");

        // Vote on the proposal
        // 0 = Against, 1 = For, 2 = Abstain
        await governor.connect(addr1).castVote(proposalId, 1);

        // Wait for voting period to end
        for (let i = 0; i < 10; i++) {
            await hre.network.provider.send("evm_mine");
        }
        
        // Check proposal state (should be Succeeded)
        // 4 = Succeeded
        expect(await governor.state(proposalId)).to.equal(4);
    });
}); 