import click
import pulsar
import json

@click.group("events")
def events_cli():
    """Commands for interacting with the platform event bus."""
    pass

@events_cli.command("listen")
@click.argument("topic")
@click.option("--subscription-name", default="platformqctl-listener", help="Name for the Pulsar subscription.")
@click.pass_context
def listen_events(ctx, topic: str, subscription_name: str):
    """
    Listens to a specific topic on the event bus and prints messages.
    
    Example: platformqctl events listen non-persistent://public/default/digital_asset_created
    """
    config = ctx.obj.get('config')
    if not config or not config.get('pulsar_url'):
        click.echo("Error: pulsar_url not found in config. Please run 'platformqctl init'.", err=True)
        return
    pulsar_url = config['pulsar_url']

    click.echo(f"Connecting to Pulsar at {pulsar_url} and listening to topic '{topic}'...")
    click.echo("Press Ctrl+C to stop.")
    
    try:
        # In a real app, the pulsar URL would come from the .platformqctl.yaml config
        client = pulsar.Client(pulsar_url)
        consumer = client.subscribe(topic, subscription_name)

        while True:
            msg = consumer.receive()
            try:
                # Try to decode as JSON for pretty printing, otherwise show raw bytes
                try:
                    data = json.loads(msg.data().decode('utf-8'))
                    click.echo(json.dumps(data, indent=2))
                except (json.JSONDecodeError, UnicodeDecodeError):
                    click.echo(f"Received raw message: {msg.data()}")
                
                consumer.acknowledge(msg)
            except Exception as e:
                click.echo(f"Error processing message: {e}", err=True)
                consumer.negative_acknowledge(msg)

    except KeyboardInterrupt:
        click.echo("\nStopping listener...")
    except Exception as e:
        click.echo(f"Failed to connect or subscribe: {e}", err=True)
    finally:
        if 'client' in locals() and client:
            client.close()
