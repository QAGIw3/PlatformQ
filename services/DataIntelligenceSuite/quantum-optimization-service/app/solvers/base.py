from abc import ABC, abstractmethod
from typing import Dict, Any

class Solver(ABC):
    """
    Abstract base class for all solvers.
    """

    @abstractmethod
    def solve(self, problem_data: Dict[str, Any], **kwargs) -> Dict[str, Any]:
        """
        Solve a given optimization problem.

        Args:
            problem_data: A dictionary containing the data that defines the problem.
            **kwargs: Additional solver-specific parameters.

        Returns:
            A dictionary containing the solution and other relevant information.
        """
        pass 