from fastapi import FastAPI, Request, HTTPException
from pydantic import BaseModel
from typing import Dict, Any

# Placeholder for the OPA policy engine
class PolicyEngine:
    def __init__(self):
        # In a real implementation, this would load policies from a file or a DB
        # and use the OPA Python client to evaluate them.
        self.policies = {
            "default": "allow = false"
        }
    
    def evaluate(self, input_data: Dict[str, Any]) -> bool:
        """Evaluates the input against the loaded policies."""
        # This is a mock evaluation. A real implementation would use OPA.
        # Example logic: by default, deny access.
        
        # A simple example policy: allow if the action is 'read' and role is 'admin'
        action = input_data.get("action")
        subject = input_data.get("subject", {})
        
        if action == "read_asset" and "admin" in subject.get("roles", []):
            return True
        
        return False

policy_engine = PolicyEngine()
app = FastAPI()

class PolicyEvaluationRequest(BaseModel):
    subject: Dict[str, Any]
    action: str
    resource: Dict[str, Any]

@app.post("/evaluate")
async def evaluate_policy(request: PolicyEvaluationRequest):
    """
    Evaluates an access control policy.
    """
    input_data = {
        "subject": request.subject,
        "action": request.action,
        "resource": request.resource,
    }
    
    allowed = policy_engine.evaluate(input_data)
    
    if not allowed:
        raise HTTPException(status_code=403, detail="Forbidden")
        
    return {"allow": True}

@app.get("/health")
async def health_check():
    return {"status": "ok"} 