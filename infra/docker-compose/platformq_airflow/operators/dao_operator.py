from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging
from typing import Any, Dict, Optional
import json
import httpx

logger = logging.getLogger(__name__)

class BaseDAOOperator(BaseOperator):
    """
    Base operator for interacting with DAO-related services.
    """

    @apply_defaults
    def __init__(
        self,
        task_id: str,
        dao_service_url: str,
        tenant_id: str,
        *args, **kwargs
    ):
        super(BaseDAOOperator, self).__init__(task_id=task_id, *args, **kwargs)
        self.dao_service_url = dao_service_url
        self.tenant_id = tenant_id

    def execute(self, context):
        raise NotImplementedError("Subclasses must implement execute method")


class DAOProposalOperator(BaseDAOOperator):
    """
    Airflow operator to create or update a DAO proposal.
    """

    @apply_defaults
    def __init__(
        self,
        task_id: str,
        dao_id: str,
        proposal_data: Dict[str, Any],
        action: str = "create", # "create" or "update"
        *args, **kwargs
    ):
        super(DAOProposalOperator, self).__init__(task_id=task_id, *args, **kwargs)
        self.dao_id = dao_id
        self.proposal_data = proposal_data
        self.action = action

    def execute(self, context):
        logger.info(f"Executing {self.action} proposal for DAO {self.dao_id}")
        url = f"{self.dao_service_url}/api/v1/daos/{self.dao_id}/proposals"

        headers = {
            "Content-Type": "application/json",
            "X-Tenant-ID": self.tenant_id,
        }

        try:
            if self.action == "create":
                response = httpx.post(url, json=self.proposal_data, headers=headers, timeout=60)
            elif self.action == "update":
                proposal_id = self.proposal_data.get("proposal_id")
                if not proposal_id:
                    raise ValueError("Proposal ID is required for update action.")
                url = f"{url}/{proposal_id}"
                response = httpx.put(url, json=self.proposal_data, headers=headers, timeout=60)
            else:
                raise ValueError(f"Unsupported action: {self.action}")

            response.raise_for_status() # Raise an exception for HTTP errors (4xx or 5xx)
            logger.info(f"Successfully {self.action}d proposal: {response.json()}")
            return response.json()

        except httpx.RequestError as e:
            logger.error(f"HTTP request failed during DAO proposal {self.action}: {e}")
            raise
        except Exception as e:
            logger.error(f"An unexpected error occurred during DAO proposal {self.action}: {e}")
            raise


class DAOVotingOperator(BaseDAOOperator):
    """
    Airflow operator to cast a vote on a DAO proposal.
    """

    @apply_defaults
    def __init__(
        self,
        task_id: str,
        dao_id: str,
        proposal_id: str,
        voter_id: str,
        vote: str, # e.g., "for", "against", "abstain"
        *args, **kwargs
    ):
        super(DAOVotingOperator, self).__init__(task_id=task_id, *args, **kwargs)
        self.dao_id = dao_id
        self.proposal_id = proposal_id
        self.voter_id = voter_id
        self.vote = vote

    def execute(self, context):
        logger.info(f"Casting vote '{self.vote}' on proposal {self.proposal_id} for DAO {self.dao_id} by voter {self.voter_id}")
        url = f"{self.dao_service_url}/api/v1/daos/{self.dao_id}/proposals/{self.proposal_id}/vote"

        headers = {
            "Content-Type": "application/json",
            "X-Tenant-ID": self.tenant_id,
        }

        payload = {
            "voter_id": self.voter_id,
            "vote": self.vote
        }

        try:
            response = httpx.post(url, json=payload, headers=headers, timeout=60)
            response.raise_for_status()
            logger.info(f"Successfully cast vote: {response.json()}")
            return response.json()

        except httpx.RequestError as e:
            logger.error(f"HTTP request failed during DAO voting: {e}")
            raise
        except Exception as e:
            logger.error(f"An unexpected error occurred during DAO voting: {e}")
            raise


class DAOExecuteProposalOperator(BaseDAOOperator):
    """
    Airflow operator to execute a DAO proposal (e.g., triggering on-chain transaction).
    This would typically be for proposals that passed voting.
    """

    @apply_defaults
    def __init__(
        self,
        task_id: str,
        dao_id: str,
        proposal_id: str,
        execution_data: Dict[str, Any],
        *args, **kwargs
    ):
        super(DAOExecuteProposalOperator, self).__init__(task_id=task_id, *args, **kwargs)
        self.dao_id = dao_id
        self.proposal_id = proposal_id
        self.execution_data = execution_data

    def execute(self, context):
        logger.info(f"Attempting to execute proposal {self.proposal_id} for DAO {self.dao_id}")
        url = f"{self.dao_service_url}/api/v1/daos/{self.dao_id}/proposals/{self.proposal_id}/execute"

        headers = {
            "Content-Type": "application/json",
            "X-Tenant-ID": self.tenant_id,
        }

        try:
            response = httpx.post(url, json=self.execution_data, headers=headers, timeout=120) # Longer timeout for on-chain ops
            response.raise_for_status()
            logger.info(f"Successfully initiated proposal execution: {response.json()}")
            return response.json()

        except httpx.RequestError as e:
            logger.error(f"HTTP request failed during DAO proposal execution: {e}")
            raise
        except Exception as e:
            logger.error(f"An unexpected error occurred during DAO proposal execution: {e}")
            raise 