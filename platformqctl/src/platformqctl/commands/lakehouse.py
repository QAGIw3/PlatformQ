import click
import json
from trino.dbapi import connect
from trino.auth import BasicAuthentication

# Configuration would come from a config file
TRINO_HOST = "localhost"
TRINO_PORT = 8080
TRINO_USER = "trino"

@click.group("lakehouse")
def lakehouse_cli():
    """Commands for interacting with the data lakehouse via Trino."""
    pass

@lakehouse_cli.command("list-schemas")
@click.option("--catalog", required=True, help="The catalog to show schemas from (e.g., hive).")
def list_schemas(catalog: str):
    """Lists all schemas within a specific lakehouse catalog."""
    click.echo(f"Fetching schemas from catalog '{catalog}'...")
    
    try:
        conn = connect(
            host=TRINO_HOST,
            port=TRINO_PORT,
            user=TRINO_USER,
            # In a real app, auth would be configured
            # auth=BasicAuthentication("user", "password"),
        )
        cur = conn.cursor()
        cur.execute(f"SHOW SCHEMAS FROM {catalog}")
        rows = cur.fetchall()
        
        click.echo(f"Schemas in '{catalog}':")
        for row in rows:
            click.echo(f"- {row[0]}")

    except Exception as e:
        click.echo(f"An error occurred while connecting to Trino: {e}", err=True)
    finally:
        if 'conn' in locals():
            conn.close() 