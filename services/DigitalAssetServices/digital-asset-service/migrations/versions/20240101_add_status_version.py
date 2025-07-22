"""add status and version to digital_assets

Revision ID: 20240101_add_status_version
Revises: 1234567890
Create Date: 2024-01-01 12:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '20240101_add_status_version'
down_revision: Union[str, None] = '1234567890'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column('digital_assets', sa.Column('status', sa.String(), nullable=False, server_default='pending'))
    op.add_column('digital_assets', sa.Column('version', sa.Integer(), nullable=False, server_default='1'))


def downgrade() -> None:
    op.drop_column('digital_assets', 'version')
    op.drop_column('digital_assets', 'status') 