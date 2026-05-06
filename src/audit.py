"""
Audit logging system for the ETL pipeline
Tracks all operations, errors, and data changes
"""

import json
import os
from datetime import datetime
from pathlib import Path
import pandas as pd
from src.utils import LOG_FILE, BASE_DIR


class AuditLogger:
    """Centralized audit logging for ETL operations."""
    
    AUDIT_DIR = os.path.join(BASE_DIR, "code_log", "audit")
    AUDIT_FILE = os.path.join(AUDIT_DIR, "audit.jsonl")
    
    def __init__(self):
        Path(self.AUDIT_DIR).mkdir(parents=True, exist_ok=True)
    
    def log_operation(self, operation_type, status, details=None, duration=None):
        """
        Log an ETL operation.
        
        Args:
            operation_type: str - 'extract', 'transform', 'load', 'monitoring'
            status: str - 'started', 'success', 'failed', 'warning'
            details: dict - Operation-specific details
            duration: float - Execution time in seconds
        """
        event = {
            "timestamp": datetime.now().isoformat(),
            "operation_type": operation_type,
            "status": status,
            "details": details or {},
            "duration": duration
        }
        
        with open(self.AUDIT_FILE, 'a', encoding='utf-8') as f:
            f.write(json.dumps(event) + '\n')
    
    def log_extraction(self, source_type, row_count, status, error=None, duration=None):
        """Log extraction operation."""
        self.log_operation(
            operation_type="extract",
            status=status,
            details={
                "source_type": source_type,
                "rows_extracted": row_count,
                "error": error
            },
            duration=duration
        )
    
    def log_transformation(self, rows_in, rows_out, rules_applied, status, error=None, duration=None):
        """Log transformation operation."""
        self.log_operation(
            operation_type="transform",
            status=status,
            details={
                "rows_input": rows_in,
                "rows_output": rows_out,
                "rules": rules_applied,
                "error": error
            },
            duration=duration
        )
    
    def log_load(self, destination, rows_loaded, status, error=None, duration=None):
        """Log load operation."""
        self.log_operation(
            operation_type="load",
            status=status,
            details={
                "destination": destination,
                "rows_loaded": rows_loaded,
                "error": error
            },
            duration=duration
        )
    
    def log_price_change(self, product_name, source, old_price, new_price, change_percent):
        """Log detected price change."""
        self.log_operation(
            operation_type="price_change",
            status="detected",
            details={
                "product": product_name,
                "source": source,
                "old_price": old_price,
                "new_price": new_price,
                "change_percent": change_percent
            }
        )
    
    def log_error(self, error_type, error_message, context=None):
        """Log an error event."""
        self.log_operation(
            operation_type="error",
            status="failed",
            details={
                "error_type": error_type,
                "error_message": str(error_message),
                "context": context
            }
        )
    
    def get_audit_history(self, operation_type=None, hours=24):
        """
        Retrieve audit history.
        
        Args:
            operation_type: str or None - Filter by operation type
            hours: int - Look back this many hours
        
        Returns:
            pd.DataFrame - Audit events
        """
        if not os.path.exists(self.AUDIT_FILE):
            return pd.DataFrame()
        
        events = []
        cutoff = pd.Timestamp.now() - pd.Timedelta(hours=hours)
        
        with open(self.AUDIT_FILE, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    event = json.loads(line)
                    event_time = pd.Timestamp(event['timestamp'])
                    
                    if event_time < cutoff:
                        continue
                    
                    if operation_type and event['operation_type'] != operation_type:
                        continue
                    
                    events.append(event)
                except json.JSONDecodeError:
                    continue
        
        if not events:
            return pd.DataFrame()
        
        df = pd.DataFrame(events)
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        return df.sort_values('timestamp', ascending=False)
    
    def get_stats(self, hours=24):
        """Get statistics for the audit period."""
        df = self.get_audit_history(hours=hours)
        
        if df.empty:
            return {}
        
        stats = {
            "total_operations": len(df),
            "successful": len(df[df['status'] == 'success']),
            "failed": len(df[df['status'] == 'failed']),
            "by_operation": df['operation_type'].value_counts().to_dict()
        }
        
        return stats


# =============================
# ERROR RECOVERY
# =============================
class ErrorRecovery:
    """Handles error recovery and retry logic."""
    
    MAX_RETRIES = 3
    RETRY_DELAY = 5  # seconds
    
    @staticmethod
    def retry_on_failure(func, max_retries=MAX_RETRIES, delay=RETRY_DELAY, backoff=True):
        """
        Retry a function with exponential backoff.
        
        Args:
            func: Callable to retry
            max_retries: Number of retry attempts
            delay: Initial delay in seconds
            backoff: Use exponential backoff
        
        Returns:
            Result from func or raises exception
        """
        import time
        
        last_error = None
        current_delay = delay
        
        for attempt in range(max_retries):
            try:
                return func()
            except Exception as e:
                last_error = e
                
                if attempt < max_retries - 1:
                    print(f"Attempt {attempt + 1} failed: {e}. Retrying in {current_delay}s...")
                    time.sleep(current_delay)
                    
                    if backoff:
                        current_delay *= 2
        
        raise Exception(f"Failed after {max_retries} attempts: {last_error}")


# =============================
# DATA VALIDATION
# =============================
class DataValidator:
    """Validates data quality at each pipeline stage."""
    
    @staticmethod
    def validate_extraction(df):
        """
        Validate extracted data.
        
        Args:
            df: pd.DataFrame - Extracted data
        
        Returns:
            dict - Validation results
        """
        results = {
            "valid": True,
            "rows": len(df),
            "columns": len(df.columns),
            "empty_percentage": (df.isnull().sum().sum() / (len(df) * len(df.columns)) * 100) if len(df) > 0 else 0,
            "issues": []
        }
        
        if df.empty:
            results["valid"] = False
            results["issues"].append("DataFrame is empty")
        
        if results["empty_percentage"] > 50:
            results["valid"] = False
            results["issues"].append(f"Too many null values ({results['empty_percentage']:.1f}%)")
        
        return results
    
    @staticmethod
    def validate_transformation(df_original, df_transformed):
        """
        Validate transformation operations.
        
        Args:
            df_original: pd.DataFrame - Original data
            df_transformed: pd.DataFrame - Transformed data
        
        Returns:
            dict - Validation results
        """
        results = {
            "valid": True,
            "rows_change": len(df_transformed) - len(df_original),
            "columns_change": len(df_transformed.columns) - len(df_original.columns),
            "issues": []
        }
        
        if len(df_transformed) > len(df_original) * 2:
            results["issues"].append("Row count increased significantly")
        
        if len(df_transformed) == 0:
            results["valid"] = False
            results["issues"].append("Transformation resulted in empty DataFrame")
        
        return results
    
    @staticmethod
    def validate_load(rows_expected, rows_actual):
        """
        Validate load operation.
        
        Args:
            rows_expected: int - Expected rows
            rows_actual: int - Actual rows loaded
        
        Returns:
            dict - Validation results
        """
        results = {
            "valid": True,
            "rows_expected": rows_expected,
            "rows_actual": rows_actual,
            "match_percent": (rows_actual / rows_expected * 100) if rows_expected > 0 else 0,
            "issues": []
        }
        
        if rows_actual < rows_expected * 0.95:
            results["valid"] = False
            results["issues"].append(f"Loaded {results['match_percent']:.1f}% of expected rows")
        
        return results


if __name__ == '__main__':
    # Example usage
    audit = AuditLogger()
    
    # Log an extraction
    audit.log_extraction(
        source_type="playwright",
        row_count=150,
        status="success",
        duration=12.5
    )
    
    # Log a transformation
    audit.log_transformation(
        rows_in=150,
        rows_out=145,
        rules_applied=["drop_nulls", "filter"],
        status="success",
        duration=2.3
    )
    
    print("✅ Audit logging system initialized")
    print(f"Stats: {audit.get_stats()}")
