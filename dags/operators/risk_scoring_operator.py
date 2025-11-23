from airflow.sdk import BaseOperator
from typing import Dict, Any, Optional


class RiskScoringOperator(BaseOperator):
    """
    Operator to calculate fraud risk scores for transactions.
    
    :param amount_threshold_high: Amount above which is high risk
    :param amount_threshold_medium: Amount above which is medium risk
    :param merchant_risk_weight: Weight for merchant risk (0-1)
    :param amount_risk_weight: Weight for amount risk (0-1)
    :param quality_penalty_weight: Weight for quality penalty (0-1)
    
    Example usage:
        score_risk = RiskScoringOperator(
            task_id='calculate_risk',
            amount_threshold_high=10000,
            amount_threshold_medium=5000
        )
    """
    
    template_fields = ('amount_threshold_high', 'amount_threshold_medium')
    ui_color = '#ffa500'
    
    def __init__(
        self,
        amount_threshold_high: int = 10000,
        amount_threshold_medium: int = 5000,
        merchant_risk_weight: float = 0.4,
        amount_risk_weight: float = 0.4,
        quality_penalty_weight: float = 0.2,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.amount_threshold_high = amount_threshold_high
        self.amount_threshold_medium = amount_threshold_medium
        self.merchant_risk_weight = merchant_risk_weight
        self.amount_risk_weight = amount_risk_weight
        self.quality_penalty_weight = quality_penalty_weight
    
    def calculate_amount_risk(self, amount: float) -> int:
        """Calculate risk score based on transaction amount (0-40 points)"""
        if amount >= self.amount_threshold_high:
            return 40
        elif amount >= self.amount_threshold_medium:
            return 25
        else:
            return int(amount / self.amount_threshold_medium * 20)
    
    def calculate_merchant_risk(self, merchant_risk: int) -> int:
        """Calculate risk score based on merchant (0-40 points)"""
        # merchant_risk is 0-100, scale to 0-40
        return int(merchant_risk * 0.4)
    
    def calculate_quality_penalty(self, quality_score: int) -> int:
        """Calculate penalty based on data quality (0-20 points penalty)"""
        # quality_score is 0-100, lower quality = higher penalty
        if quality_score < 50:
            return 20
        elif quality_score < 75:
            return 10
        else:
            return 0
    
    def determine_severity(self, risk_score: float) -> str:
        """Determine severity level based on risk score"""
        if risk_score >= 80:
            return 'CRITICAL'
        elif risk_score >= 60:
            return 'HIGH'
        elif risk_score >= 40:
            return 'MEDIUM'
        else:
            return 'LOW'
    
    def execute(self, context: Dict[str, Any]):
        """
        Calculate risk scores for transactions pulled from XCom
        
        TODO: Implement the following:
        1. Pull transactions from upstream task (use context['ti'].xcom_pull())
        2. For each transaction, calculate:
           - Amount risk score
           - Merchant risk score (if available)
           - Quality penalty (if quality_score available)
           - Composite risk score
           - Severity level
        3. Add risk fields to each transaction
        4. Log summary statistics
        5. Return enhanced transactions
        """
        
        self.log.info("🎯 Starting risk scoring...")
        self.log.info(f"   Thresholds: High=${self.amount_threshold_high}, Medium=${self.amount_threshold_medium}")

        ti = context['ti']
        transactions = ti.xcom_pull(task_ids='generate_test_transactions')

        for txn in transactions:
            amount_risk = self.calculate_amount_risk(txn['amount'])
            merchant_risk = self.calculate_merchant_risk(txn.get('merchant_risk', 50))
            quality_penalty = self.calculate_quality_penalty(txn.get('quality_score', 100))
            composite_risk = amount_risk + merchant_risk - quality_penalty
            txn['risk_score'] = composite_risk
            txn['severity'] = self.determine_severity(composite_risk)
        
        # TODO: Log summary
        # Hint: Count transactions by severity level
        tiered_txn = {
            'CRITICAL': [t for t in transactions if t['severity'] == 'CRITICAL'],
            'HIGH': [t for t in transactions if t['severity'] == 'HIGH'],
            'MEDIUM': [t for t in transactions if t['severity'] == 'MEDIUM'],
            'LOW': [t for t in transactions if t['severity'] == 'LOW']
        }
        
        for tier, txns in tiered_txn.items():
            c = len(txns)
            if c == 0:
                print(f'{tier}: 0 transactions.')
            else:
                avg_risk = sum([t['risk_score'] for t in txns])/len(txns)
                print(f"{tier}: {c} transactions with avg risk score {avg_risk}.")
        
        return transactions