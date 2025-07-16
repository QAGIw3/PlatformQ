"""add vc fields to digital_assets

Revision ID: 20240101_add_vc_fields
Revises: 20240101_add_status_version
Create Date: 2024-01-01 14:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision: str = '20240101_add_vc_fields'
down_revision: Union[str, None] = '20240101_add_status_version'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Add VC-related columns
    op.add_column('digital_assets', sa.Column('creation_vc_id', sa.String(), nullable=True))
    op.add_column('digital_assets', sa.Column('lineage_vc_ids', postgresql.JSONB(), nullable=True, server_default='[]'))
    op.add_column('digital_assets', sa.Column('latest_processing_vc_id', sa.String(), nullable=True))
    
    # Create indexes for efficient VC lookups
    op.create_index('idx_creation_vc_id', 'digital_assets', ['creation_vc_id'])
    op.create_index('idx_latest_processing_vc_id', 'digital_assets', ['latest_processing_vc_id'])


def downgrade() -> None:
    # Drop indexes
    op.drop_index('idx_latest_processing_vc_id', table_name='digital_assets')
    op.drop_index('idx_creation_vc_id', table_name='digital_assets')
    
    # Drop columns
    op.drop_column('digital_assets', 'latest_processing_vc_id')
    op.drop_column('digital_assets', 'lineage_vc_ids')
    op.drop_column('digital_assets', 'creation_vc_id') 