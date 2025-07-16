"""Add CAD collaboration tables

Revision ID: add_cad_collab_001
Revises: 
Create Date: 2024-01-01 00:00:00.000000

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = 'add_cad_collab_001'
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Create cad_sessions table
    op.create_table('cad_sessions',
        sa.Column('session_id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('asset_id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('tenant_id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('created_by', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=True),
        sa.Column('closed_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('active_users', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column('crdt_state', sa.LargeBinary(), nullable=True),
        sa.Column('vector_clock', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column('operation_count', sa.Integer(), nullable=True),
        sa.Column('last_checkpoint_id', postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column('session_metadata', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.ForeignKeyConstraint(['asset_id'], ['digital_assets.asset_id'], ),
        sa.PrimaryKeyConstraint('session_id')
    )
    op.create_index(op.f('ix_cad_sessions_tenant_id'), 'cad_sessions', ['tenant_id'], unique=False)
    
    # Create cad_checkpoints table
    op.create_table('cad_checkpoints',
        sa.Column('checkpoint_id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('session_id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=True),
        sa.Column('snapshot_uri', sa.String(), nullable=False),
        sa.Column('operation_range_start', sa.String(), nullable=False),
        sa.Column('operation_range_end', sa.String(), nullable=False),
        sa.Column('operation_count', sa.Integer(), nullable=False),
        sa.Column('crdt_state_size', sa.BigInteger(), nullable=False),
        sa.ForeignKeyConstraint(['session_id'], ['cad_sessions.session_id'], ),
        sa.PrimaryKeyConstraint('checkpoint_id')
    )
    
    # Create geometry_versions table
    op.create_table('geometry_versions',
        sa.Column('version_id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('asset_id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('version_number', sa.Integer(), nullable=False),
        sa.Column('created_by', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=True),
        sa.Column('geometry_snapshot_uri', sa.String(), nullable=True),
        sa.Column('geometry_diff', sa.LargeBinary(), nullable=True),
        sa.Column('parent_version_id', postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column('vertex_count', sa.Integer(), nullable=True),
        sa.Column('edge_count', sa.Integer(), nullable=True),
        sa.Column('face_count', sa.Integer(), nullable=True),
        sa.Column('file_size_bytes', sa.BigInteger(), nullable=True),
        sa.Column('version_metadata', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column('commit_message', sa.String(), nullable=True),
        sa.ForeignKeyConstraint(['asset_id'], ['digital_assets.asset_id'], ),
        sa.PrimaryKeyConstraint('version_id'),
        sa.UniqueConstraint('asset_id', 'version_number', name='_asset_version_uc')
    )


def downgrade() -> None:
    op.drop_table('geometry_versions')
    op.drop_table('cad_checkpoints')
    op.drop_index(op.f('ix_cad_sessions_tenant_id'), table_name='cad_sessions')
    op.drop_table('cad_sessions') 