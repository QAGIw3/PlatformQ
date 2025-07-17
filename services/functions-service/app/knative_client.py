from kubernetes import client, config
from ..schemas.function import Function, FunctionCreate
from .containerizer import build_and_push_image

class KnativeClient:
    def __init__(self):
        config.load_incluster_config()
        self.api = client.CustomObjectsApi()

    def deploy_function(self, function: FunctionCreate, code: str, requirements: str) -> Function:
        image_tag = build_and_push_image(function.name, code, requirements)
        
        knative_service = {
            "apiVersion": "serving.knative.dev/v1",
            "kind": "Service",
            "metadata": {
                "name": function.name,
                "namespace": "default",
            },
            "spec": {
                "template": {
                    "spec": {
                        "containers": [
                            {
                                "image": image_tag,
                                "env": [{"name": k, "value": v} for k, v in function.env.items()] if function.env else [],
                            }
                        ]
                    }
                }
            },
        }
        
        self.api.create_namespaced_custom_object(
            group="serving.knative.dev",
            version="v1",
            namespace="default",
            plural="services",
            body=knative_service,
        )
        
        # In a real app, you would wait for the service to be ready
        # and then get the URL from the status.
        return Function(**function.dict(), url=f"http://{function.name}.default.example.com")

    def list_functions(self) -> list[Function]:
        response = self.api.list_namespaced_custom_object(
            group="serving.knative.dev",
            version="v1",
            namespace="default",
            plural="services",
        )
        return [
            Function(
                name=item["metadata"]["name"],
                image=item["spec"]["template"]["spec"]["containers"][0]["image"],
                url=item["status"]["url"],
            )
            for item in response["items"]
        ]

    def get_function(self, name: str) -> Function | None:
        try:
            response = self.api.get_namespaced_custom_object(
                group="serving.knative.dev",
                version="v1",
                namespace="default",
                plural="services",
                name=name,
            )
            return Function(
                name=response["metadata"]["name"],
                image=response["spec"]["template"]["spec"]["containers"][0]["image"],
                url=response["status"]["url"],
            )
        except client.ApiException as e:
            if e.status == 404:
                return None
            raise

    def delete_function(self, name: str):
        self.api.delete_namespaced_custom_object(
            group="serving.knative.dev",
            version="v1",
            namespace="default",
            plural="services",
            name=name,
            body=client.V1DeleteOptions(),
        )

    def invoke_function(self, name: str):
        function = self.get_function(name)
        if not function:
            raise Exception("Function not found")
            
        # In a real app, you would use an HTTP client to invoke the function's URL
        print(f"Invoking function {name} at {function.url}")
        return {"status": "invoked"} 