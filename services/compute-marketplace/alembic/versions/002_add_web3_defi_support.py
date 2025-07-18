"""Add Web3/DeFi support

Revision ID: 002_add_web3_defi_support
Revises: 001_initial_schema
Create Date: 2024-01-01 00:00:00.000000

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '002_add_web3_defi_support'
down_revision = '001_initial_schema'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Add Web3/DeFi columns to compute_offerings table
    op.add_column('compute_offerings', 
        sa.Column('is_tokenized', sa.Boolean(), nullable=False, server_default='false'))
    op.add_column('compute_offerings',
        sa.Column('token_address', sa.String(255), nullable=True))
    op.add_column('compute_offerings',
        sa.Column('total_supply', sa.Integer(), nullable=True))
    op.add_column('compute_offerings',
        sa.Column('circulating_supply', sa.Integer(), nullable=True, server_default='0'))
    op.add_column('compute_offerings',
        sa.Column('liquidity_pool_address', sa.String(255), nullable=True))
    op.add_column('compute_offerings',
        sa.Column('staking_pool_address', sa.String(255), nullable=True))
    op.add_column('compute_offerings',
        sa.Column('min_fraction_size', sa.Float(), nullable=True, server_default='0.1'))
    op.add_column('compute_offerings',
        sa.Column('fractionalization_enabled', sa.Boolean(), nullable=False, server_default='true'))
    op.add_column('compute_offerings',
        sa.Column('ownership_nft_address', sa.String(255), nullable=True))
    
    # Create index for tokenized offerings
    op.create_index('idx_offering_tokenized', 'compute_offerings', ['is_tokenized', 'status'])
    
    # Create compute_token_holders table
    op.create_table('compute_token_holders',
        sa.Column('id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('offering_id', sa.String(255), nullable=False),
        sa.Column('wallet_address', sa.String(255), nullable=False),
        sa.Column('token_balance', sa.Float(), nullable=False),
        sa.Column('locked_balance', sa.Float(), nullable=False, server_default='0'),
        sa.Column('staked_amount', sa.Float(), nullable=False, server_default='0'),
        sa.Column('stake_start_time', sa.DateTime(), nullable=True),
        sa.Column('accumulated_rewards', sa.Float(), nullable=False, server_default='0'),
        sa.Column('last_reward_claim', sa.DateTime(), nullable=True),
        sa.Column('voting_power', sa.Float(), nullable=False, server_default='0'),
        sa.Column('delegation_address', sa.String(255), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.Column('updated_at', sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(['offering_id'], ['compute_offerings.offering_id'], ),
        sa.PrimaryKeyConstraint('id')
    )
    
    op.create_index('idx_token_holder_wallet', 'compute_token_holders', ['wallet_address', 'offering_id'])
    op.create_index('idx_token_balance', 'compute_token_holders', ['token_balance'])
    
    # Create compute_liquidity_positions table
    op.create_table('compute_liquidity_positions',
        sa.Column('id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('position_id', sa.String(255), nullable=False, unique=True),
        sa.Column('offering_id', sa.String(255), nullable=False),
        sa.Column('wallet_address', sa.String(255), nullable=False),
        sa.Column('compute_token_amount', sa.Float(), nullable=False),
        sa.Column('paired_token_amount', sa.Float(), nullable=False),
        sa.Column('paired_token_symbol', sa.String(10), nullable=False),
        sa.Column('lp_token_amount', sa.Float(), nullable=False),
        sa.Column('pool_share_percentage', sa.Float(), nullable=True),
        sa.Column('unclaimed_fees', sa.Float(), nullable=False, server_default='0'),
        sa.Column('unclaimed_rewards', sa.Float(), nullable=False, server_default='0'),
        sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.Column('updated_at', sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(['offering_id'], ['compute_offerings.offering_id'], ),
        sa.PrimaryKeyConstraint('id')
    )
    
    # Create compute_derivatives table
    op.create_table('compute_derivatives',
        sa.Column('id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('contract_id', sa.String(255), nullable=False, unique=True),
        sa.Column('offering_id', sa.String(255), nullable=False),
        sa.Column('derivative_type', sa.String(50), nullable=False),
        sa.Column('strike_price', sa.Float(), nullable=True),
        sa.Column('expiry_date', sa.DateTime(), nullable=True),
        sa.Column('settlement_type', sa.String(20), nullable=True),
        sa.Column('mark_price', sa.Float(), nullable=True),
        sa.Column('index_price', sa.Float(), nullable=True),
        sa.Column('funding_rate', sa.Float(), nullable=True),
        sa.Column('open_interest', sa.Float(), nullable=True),
        sa.Column('volume_24h', sa.Float(), nullable=True),
        sa.Column('initial_margin', sa.Float(), nullable=True),
        sa.Column('maintenance_margin', sa.Float(), nullable=True),
        sa.Column('max_leverage', sa.Integer(), nullable=True, server_default='10'),
        sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.Column('updated_at', sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(['offering_id'], ['compute_offerings.offering_id'], ),
        sa.PrimaryKeyConstraint('id')
    )
    
    op.create_index('idx_derivative_type', 'compute_derivatives', ['derivative_type', 'expiry_date'])
    op.create_index('idx_derivative_offering', 'compute_derivatives', ['offering_id'])
    
    # Create compute_yield_strategies table
    op.create_table('compute_yield_strategies',
        sa.Column('id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('strategy_id', sa.String(255), nullable=False, unique=True),
        sa.Column('name', sa.String(255), nullable=False),
        sa.Column('strategy_type', sa.String(50), nullable=False),
        sa.Column('target_offerings', sa.JSON(), nullable=False, server_default='[]'),
        sa.Column('current_apy', sa.Float(), nullable=True),
        sa.Column('total_value_locked', sa.Float(), nullable=True),
        sa.Column('total_rewards_distributed', sa.Float(), nullable=True),
        sa.Column('risk_score', sa.Float(), nullable=True),
        sa.Column('impermanent_loss_protection', sa.Boolean(), nullable=False, server_default='false'),
        sa.Column('is_active', sa.Boolean(), nullable=False, server_default='true'),
        sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.Column('updated_at', sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.PrimaryKeyConstraint('id')
    )
    
    # Create compute_lending_pools table
    op.create_table('compute_lending_pools',
        sa.Column('id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('pool_id', sa.String(255), nullable=False, unique=True),
        sa.Column('offering_id', sa.String(255), nullable=False),
        sa.Column('total_supplied', sa.Float(), nullable=False, server_default='0'),
        sa.Column('total_borrowed', sa.Float(), nullable=False, server_default='0'),
        sa.Column('utilization_rate', sa.Float(), nullable=False, server_default='0'),
        sa.Column('supply_apy', sa.Float(), nullable=True),
        sa.Column('borrow_apy', sa.Float(), nullable=True),
        sa.Column('collateral_factor', sa.Float(), nullable=False, server_default='0.75'),
        sa.Column('liquidation_threshold', sa.Float(), nullable=False, server_default='0.85'),
        sa.Column('liquidation_penalty', sa.Float(), nullable=False, server_default='0.05'),
        sa.Column('created_at', sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.Column('updated_at', sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(['offering_id'], ['compute_offerings.offering_id'], ),
        sa.PrimaryKeyConstraint('id')
    )


def downgrade() -> None:
    # Drop tables in reverse order
    op.drop_table('compute_lending_pools')
    op.drop_table('compute_yield_strategies')
    op.drop_table('compute_derivatives')
    op.drop_table('compute_liquidity_positions')
    op.drop_table('compute_token_holders')
    
    # Drop indexes
    op.drop_index('idx_offering_tokenized', 'compute_offerings')
    
    # Drop columns from compute_offerings
    op.drop_column('compute_offerings', 'ownership_nft_address')
    op.drop_column('compute_offerings', 'fractionalization_enabled')
    op.drop_column('compute_offerings', 'min_fraction_size')
    op.drop_column('compute_offerings', 'staking_pool_address')
    op.drop_column('compute_offerings', 'liquidity_pool_address')
    op.drop_column('compute_offerings', 'circulating_supply')
    op.drop_column('compute_offerings', 'total_supply')
    op.drop_column('compute_offerings', 'token_address')
    op.drop_column('compute_offerings', 'is_tokenized')