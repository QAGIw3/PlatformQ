"""add marketplace fields

Revision ID: marketplace_001
Revises: 20240101_add_vc_fields
Create Date: 2024-01-02 10:00:00.000000

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = 'marketplace_001'
down_revision = '20240101_add_vc_fields'
branch_labels = None
depends_on = None


def upgrade():
    # Add marketplace columns to digital_assets table
    op.add_column('digital_assets', sa.Column('is_for_sale', sa.Boolean(), nullable=True, server_default='false'))
    op.add_column('digital_assets', sa.Column('sale_price', sa.Numeric(precision=20, scale=6), nullable=True))
    op.add_column('digital_assets', sa.Column('is_licensable', sa.Boolean(), nullable=True, server_default='false'))
    op.add_column('digital_assets', sa.Column('license_terms', postgresql.JSONB(astext_type=sa.Text()), nullable=True))
    op.add_column('digital_assets', sa.Column('royalty_percentage', sa.Integer(), nullable=True, server_default='250'))
    op.add_column('digital_assets', sa.Column('blockchain_address', sa.String(), nullable=True))
    op.add_column('digital_assets', sa.Column('smart_contract_addresses', postgresql.JSONB(astext_type=sa.Text()), nullable=True, server_default='{}'))
    
    # Create indexes for marketplace queries
    op.create_index('idx_digital_assets_for_sale', 'digital_assets', ['is_for_sale'])
    op.create_index('idx_digital_assets_licensable', 'digital_assets', ['is_licensable'])
    op.create_index('idx_digital_assets_owner_blockchain', 'digital_assets', ['blockchain_address'])


def downgrade():
    # Remove indexes
    op.drop_index('idx_digital_assets_owner_blockchain', table_name='digital_assets')
    op.drop_index('idx_digital_assets_licensable', table_name='digital_assets')
    op.drop_index('idx_digital_assets_for_sale', table_name='digital_assets')
    
    # Remove columns
    op.drop_column('digital_assets', 'smart_contract_addresses')
    op.drop_column('digital_assets', 'blockchain_address')
    op.drop_column('digital_assets', 'royalty_percentage')
    op.drop_column('digital_assets', 'license_terms')
    op.drop_column('digital_assets', 'is_licensable')
    op.drop_column('digital_assets', 'sale_price')
    op.drop_column('digital_assets', 'is_for_sale') 