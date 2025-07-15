from shared_lib.base_service import create_base_app
from .api import endpoints
from .api.deps import (
    get_db_session, 
    get_api_key_crud_placeholder, 
    get_user_crud_placeholder, 
    get_password_verifier_placeholder
)
import logging

# --- gRPC Server Setup (Example) ---
# To enable gRPC for this service:
# 1. Uncomment the gRPC dependencies in `requirements.txt`.
# 2. Add your .proto file to `libs/shared/protos`.
# 3. Run the gRPC generation script: `bash services/__SERVICE_NAME__/scripts/generate_grpc.sh`.
# 4. Implement your servicer logic in a class (e.g., `YourServiceServicer`).
# 5. Pass an instance of your servicer class and the `add_..._to_server` function 
#    to the `create_base_app` factory below.
#
# Example:
#
# from .grpc_generated import your_service_pb2_grpc
#
# class YourServiceServicer(your_service_pb2_grpc.YourServiceServicer):
#      def YourMethod(self, request, context):
#          return your_service_pb2.YourResponse(reply="Hello, " + request.name)
#
# app = create_base_app(
#     ...,
#     grpc_servicer=YourServiceServicer(),
#     grpc_add_servicer_func=your_service_pb2_grpc.add_YourServiceServicer_to_server,
#     grpc_port=50051  # Use a unique port for each service
# )

app = create_base_app(
    service_name="__SERVICE_NAME__",
    db_session_dependency=get_db_session,
    api_key_crud_dependency=get_api_key_crud_placeholder,
    user_crud_dependency=get_user_crud_placeholder,
    password_verifier_dependency=get_password_verifier_placeholder,
)

# Include service-specific routers
app.include_router(endpoints.router, prefix="/api/v1", tags=["__SERVICE_NAME__"])

@app.on_event("startup")
async def startup_event():
    logging.basicConfig(level=logging.INFO)
    logging.info("Starting up __SERVICE_NAME__...")

# Service-specific root endpoint
@app.get("/")
def read_root():
    return {"message": "__SERVICE_NAME__ is running"}
