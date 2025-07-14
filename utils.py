# utils.py
"""
Utility functions and classes for Atlan S3 Connector
Provides common functionality, performance monitoring, and helper methods
"""

import logging
import time
import json
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict
import os
from contextlib import contextmanager

from atlan_client import get_atlan_client


logger = logging.getLogger(__name__)

@dataclass
class PerformanceMetric:
    """Performance metric data structure"""
    operation: str
    start_time: float
    end_time: float
    duration: float
    success: bool
    error_message: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None

class PerformanceMonitor:
    """Performance monitoring and metrics collection"""

    def __init__(self):
        self.metrics: List[PerformanceMetric] = []
        self.active_operations: Dict[str, float] = {}

    @contextmanager
    def measure(self, operation_name: str, metadata: Optional[Dict[str, Any]] = None):
        """Context manager for measuring operation performance"""
        start_time = time.time()
        self.active_operations[operation_name] = start_time

        try:
            yield
            end_time = time.time()
            duration = end_time - start_time
            metric = PerformanceMetric(
                operation=operation_name,
                start_time=start_time,
                end_time=end_time,
                duration=duration,
                success=True,
                metadata=metadata,
            )
            self.metrics.append(metric)
            logger.info(f"Operation '{operation_name}' completed in {duration:.2f} seconds")

        except Exception as e:
            end_time = time.time()
            duration = end_time - start_time
            metric = PerformanceMetric(
                operation=operation_name,
                start_time=start_time,
                end_time=end_time,
                duration=duration,
                success=False,
                error_message=str(e),
                metadata=metadata,
            )
            self.metrics.append(metric)
            logger.error(f"Operation '{operation_name}' failed after {duration:.2f} seconds: {str(e)}")
            raise

        finally:
            if operation_name in self.active_operations:
                del self.active_operations[operation_name]

    def get_metrics(self) -> Dict[str, Any]:
        """Get comprehensive performance metrics"""
        if not self.metrics:
            return {"total_operations": 0, "total_duration": 0, "success_rate": 0}

        total_operations = len(self.metrics)
        successful_operations = sum(1 for m in self.metrics if m.success)
        total_duration = sum(m.duration for m in self.metrics)
        average_duration = total_duration / total_operations if total_operations > 0 else 0
        success_rate = (successful_operations / total_operations) * 100 if total_operations > 0 else 0

        return {
            "total_operations": total_operations,
            "successful_operations": successful_operations,
            "failed_operations": total_operations - successful_operations,
            "total_duration": total_duration,
            "average_duration": average_duration,
            "success_rate": success_rate,
        }

class AtlanUtils:
    """Utility functions for Atlan operations"""

    def __init__(self):
        self.atlan_client = get_atlan_client()
        self.last_run_file = "last_run_timestamp.txt"

    async def get_last_run_timestamp(self) -> datetime:
        """Get the timestamp of the last successful run"""
        try:
            if os.path.exists(self.last_run_file):
                with open(self.last_run_file, 'r') as f:
                    timestamp_str = f.read().strip()
                    return datetime.fromisoformat(timestamp_str)
            else:
                return datetime.now() - timedelta(days=30)
        except Exception as e:
            logger.error(f"Failed to get last run timestamp: {str(e)}")
            return datetime.now() - timedelta(days=30)

    async def update_last_run_timestamp(self) -> None:
        """Update the last run timestamp to current time"""
        try:
            current_time = datetime.now()
            with open(self.last_run_file, 'w') as f:
                f.write(current_time.isoformat())
            logger.info(f"Updated last run timestamp to {current_time.isoformat()}")
        except Exception as e:
            logger.error(f"Failed to update last run timestamp: {str(e)}")

