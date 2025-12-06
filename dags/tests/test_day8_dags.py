import pytest
from airflow.models import DagBag


class TestDagValidation:
    """Test that DAGs load without errors."""
    
    @pytest.fixture(scope="class")
    def dagbag(self):
        return DagBag(include_examples=False)
    
    def test_dag_loads_no_errors(self, dagbag):
        """Check no import errors in Day 8 DAGs specifically."""
        day8_errors = {k: v for k, v in dagbag.import_errors.items() if 'day8' in k}
        assert len(day8_errors) == 0, f"DAG import errors: {day8_errors}"
    
    def test_taskgroups_dag_exists(self, dagbag):
        """Check our TaskGroups DAG loaded."""
        assert 'day8_taskgroups_fraud' in dagbag.dags
    
    def test_file_passing_dag_exists(self, dagbag):
        """Check our file passing DAG loaded."""
        assert 'day8_file_passing' in dagbag.dags


class TestDagStructure:
    """Test DAG structure and dependencies."""
    
    @pytest.fixture(scope="class")
    def dagbag(self):
        return DagBag(include_examples=False)
    
    def test_taskgroups_dag_task_count(self, dagbag):
        """TaskGroups DAG should have 9 tasks."""
        dag = dagbag.dags['day8_taskgroups_fraud']
        # TODO: Assert the correct number of tasks
        assert len(dag.tasks) == 9, f"Expected 9 tasks, got {len(dag.tasks)}"
    
    def test_file_passing_dag_task_count(self, dagbag):
        """File passing DAG should have 3 tasks."""
        dag = dagbag.dags['day8_file_passing']
        assert len(dag.tasks) == 3, f"Expected 3 tasks, got {len(dag.tasks)}"


class TestTaskLogic:
    """Test individual task functions."""
    
    def test_high_risk_detection(self):
        """Test that high-risk detection works correctly."""
        # Simulate transaction data
        test_txns = [
            {"id": 1, "amount": 100},    # Low risk
            {"id": 2, "amount": 6000},   # High risk
            {"id": 3, "amount": 5001},   # High risk
            {"id": 4, "amount": 5000},   # Boundary - NOT high risk
        ]
        
        # Your logic: count where amount > 5000
        high_risk_count = len([t for t in test_txns if t['amount'] > 5000])
        
        assert high_risk_count == 2, f"Expected 2 high-risk, got {high_risk_count}"