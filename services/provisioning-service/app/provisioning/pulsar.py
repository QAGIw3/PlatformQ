from pulsar.admin import PulsarAdmin

def create_pulsar_namespace(pulsar_admin: PulsarAdmin, tenant_id: str):
    """
    Creates a new namespace for a tenant and sets permissions.
    """
    namespace = f"platformq/{tenant_id}"
    
    try:
        pulsar_admin.create_namespace(namespace)
    except Exception as e:
        # This will fail if the namespace already exists, which is fine.
        # In a real app, you would want more robust error handling.
        print(f"Could not create namespace {namespace}: {e}")

    # Grant permissions to the services that need to access this namespace.
    # In a real app, you would have a more sophisticated way of managing
    # these roles and permissions.
    roles = ["auth-service", "digital-asset-service", "governance-service", "projects-service"]
    for role in roles:
        try:
            pulsar_admin.grant_permission(namespace, role, ["produce", "consume"])
        except Exception as e:
            print(f"Could not grant permission to role {role} on namespace {namespace}: {e}") 