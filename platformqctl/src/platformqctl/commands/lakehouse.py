import click
import json
from trino.dbapi import connect
from trino.auth import BasicAuthentication
from tabulate import tabulate

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
@click.pass_context
def list_schemas(ctx, catalog: str):
    """Lists all schemas within a specific lakehouse catalog."""
    config = ctx.obj.get('config')
    if not config or not config.get('trino_host'):
        click.echo("Error: trino_host not found in config. Please run 'platformqctl init'.", err=True)
        return
    trino_host = config['trino_host']

    click.echo(f"Fetching schemas from catalog '{catalog}' at host '{trino_host}'...")
    
    try:
        conn = connect(
            host=trino_host,
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

@lakehouse_cli.command("query")
@click.argument("sql_query")
@click.pass_context
def query_lakehouse(ctx, sql_query: str):
    """Executes a SQL query against the lakehouse."""
    config = ctx.obj.get('config')
    if not config or not config.get('trino_host'):
        click.echo("Error: trino_host not found in config. Please run 'platformqctl init'.", err=True)
        return
    trino_host = config['trino_host']

    click.echo("Executing query...")
    
    try:
        conn = connect(
            host=trino_host,
            port=TRINO_PORT,
            user=TRINO_USER,
        )
        cur = conn.cursor()
        cur.execute(sql_query)
        rows = cur.fetchall()
        
        if rows:
            headers = [desc[0] for desc in cur.description]
            click.echo(tabulate(rows, headers=headers, tablefmt="psql"))
        else:
            click.echo("Query executed successfully, no rows returned.")

    except Exception as e:
        click.echo(f"An error occurred during query execution: {e}", err=True)
    finally:
        if 'conn' in locals():
            conn.close() 