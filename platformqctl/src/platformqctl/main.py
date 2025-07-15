import click
import os
import shutil
from ruamel.yaml import YAML
from jinja2 import Environment, FileSystemLoader

TEMPLATE_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "templates", "service-template")
SERVICES_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "services")
PLACEHOLDER = "__SERVICE_NAME__"

STACK_CHART_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "iac", "kubernetes", "charts", "platformq-stack")
CI_WORKFLOW_FILE = os.path.join(os.path.dirname(__file__), "..", "..", ".github", "workflows", "ci.yml")

CONFIG_FILE_NAME = ".platformqctl.yaml"
yaml = YAML()

# --- Helper Functions ---
def load_config():
    if not os.path.exists(CONFIG_FILE_NAME):
        return None
    with open(CONFIG_FILE_NAME, 'r') as f:
        return yaml.load(f)

def render_templates(target_dir, context):
    env = Environment(loader=FileSystemLoader(target_dir))
    for dirpath, _, filenames in os.walk(target_dir):
        for filename in filenames:
            file_path = os.path.join(dirpath, filename)
            template = env.get_template(os.path.relpath(file_path, target_dir))
            output = template.render(context)
            with open(file_path, 'w') as f:
                f.write(output)

# --- CLI Commands ---
@click.group()
def cli():
    """A CLI tool for managing the platformQ microservices."""
    pass

@cli.command()
@click.pass_context
def init(ctx):
    """Initializes a new config file for platformqctl."""
    config_data = {
        "enterprise": {
            "registry_url": "gcr.io/your-company",
            "ci_system": "github_actions",
            "helm_repo": "https://charts.your-company.com",
        },
        "template_repo": "https://github.com/your-company/service-templates.git"
    }
    with open(CONFIG_FILE_NAME, 'w') as f:
        yaml.dump(config_data, f)
    click.echo(f"Initialized config file at ./{CONFIG_FILE_NAME}")

@cli.command()
@click.argument('service_name')
def create_service(service_name):
    """Creates a new service from the configured template."""
    config = load_config()
    if not config:
        click.echo("Error: Config file not found. Please run 'platformqctl init'.")
        return
    
    target_dir = os.path.join(SERVICES_DIR, service_name)
    if os.path.exists(target_dir):
        click.echo(f"Error: Service '{service_name}' already exists.")
        return

    # 1. Copy the template directory
    shutil.copytree(TEMPLATE_DIR, target_dir)

    # 2. Walk through the new directory and replace placeholders
    for dirpath, _, filenames in os.walk(target_dir):
        for filename in filenames:
            file_path = os.path.join(dirpath, filename)
            with open(file_path, 'r') as f:
                content = f.read()
            
            content = content.replace(PLACEHOLDER, service_name)
            
            with open(file_path, 'w') as f:
                f.write(content)
                
    if deployable:
        click.echo(f"Adding '{service_name}' to platform stack...")
        
        # 1. Add to umbrella Helm chart
        chart_path = os.path.join(STACK_CHART_DIR, "Chart.yaml")
        with open(chart_path, 'r') as f:
            chart = yaml.load(f)
        chart['dependencies'].append({
            'name': service_name,
            'version': '0.1.0',
            'repository': f'file://./../{service_name}'
        })
        with open(chart_path, 'w') as f:
            yaml.dump(chart, f)
        
        # 2. Add to umbrella values.yaml
        values_path = os.path.join(STACK_CHART_DIR, "values.yaml")
        with open(values_path, 'a') as f:
            f.write(f"\n{service_name}:\n  replicaCount: 1\n  image:\n    repository: gcr.io/your-repo/{service_name}\n    tag: latest\n")

        # 3. Add to CI/CD workflow
        with open(CI_WORKFLOW_FILE, 'r') as f:
            ci_config = yaml.load(f)
        # (Conceptual: logic to copy the existing 'build-and-deploy-auth-service' job
        # and replace 'auth-service' with the new service_name would go here)
        click.echo("  - CI/CD job definition added (conceptual).")

    click.echo(f"Service '{service_name}' created successfully.")

if __name__ == '__main__':
    cli() 