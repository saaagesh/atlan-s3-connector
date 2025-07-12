# utils.py
"""
Utility functions and classes for Atlan S3 Connector
Provides common functionality, performance monitoring, and helper methods
"""

import logging
import time
import json
import asyncio
from typing import Dict, List, Any, Optional, Union
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict
import os
from contextlib import contextmanager

from pyatlan.client.atlan import AtlanClient
from pyatlan.model.assets import Asset
from pyatlan.model.query import Query

from config import AtlanConfig

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
            # Operation succeeded
            end_time = time.time()
            duration = end_time - start_time

            metric = PerformanceMetric(
                operation=operation_name,
                start_time=start_time,
                end_time=end_time,
                duration=duration,
                success=True,
                metadata=metadata
            )
            self.metrics.append(metric)

            logger.info(f"Operation '{operation_name}' completed in {duration:.2f} seconds")

        except Exception as e:
            # Operation failed
            end_time = time.time()
            duration = end_time - start_time

            metric = PerformanceMetric(
                operation=operation_name,
                start_time=start_time,
                end_time=end_time,
                duration=duration,
                success=False,
                error_message=str(e),
                metadata=metadata
            )
            self.metrics.append(metric)

            logger.error(f"Operation '{operation_name}' failed after {duration:.2f} seconds: {str(e)}")
            raise

        finally:
            # Clean up active operations
            if operation_name in self.active_operations:
                del self.active_operations[operation_name]

    def get_metrics(self) -> Dict[str, Any]:
        """Get comprehensive performance metrics"""
        if not self.metrics:
            return {"total_operations": 0, "total_duration": 0, "success_rate": 0}

        total_operations = len(self.metrics)
        successful_operations = sum(1 for m in self.metrics if m.success)
        total_duration = sum(m.duration for m in self.metrics)
        average_duration = total_duration / total_operations
        success_rate = (successful_operations / total_operations) * 100

        # Group metrics by operation type
        operation_stats = {}
        for metric in self.metrics:
            if metric.operation not in operation_stats:
                operation_stats[metric.operation] = {
                    "count": 0,
                    "total_duration": 0,
                    "successes": 0,
                    "failures": 0
                }

            stats = operation_stats[metric.operation]
            stats["count"] += 1
            stats["total_duration"] += metric.duration

            if metric.success:
                stats["successes"] += 1
            else:
                stats["failures"] += 1

        # Calculate averages for each operation
        for operation, stats in operation_stats.items():
            stats["average_duration"] = stats["total_duration"] / stats["count"]
            stats["success_rate"] = (stats["successes"] / stats["count"]) * 100

        return {
            "total_operations": total_operations,
            "successful_operations": successful_operations,
            "failed_operations": total_operations - successful_operations,
            "total_duration": total_duration,
            "average_duration": average_duration,
            "success_rate": success_rate,
            "operation_breakdown": operation_stats,
            "slowest_operations": self._get_slowest_operations(),
            "recent_failures": self._get_recent_failures()
        }

    def _get_slowest_operations(self, limit: int = 5) -> List[Dict[str, Any]]:
        """Get the slowest operations"""
        sorted_metrics = sorted(self.metrics, key=lambda m: m.duration, reverse=True)
        return [
            {
                "operation": m.operation,
                "duration": m.duration,
                "timestamp": datetime.fromtimestamp(m.start_time).isoformat(),
                "success": m.success
            }
            for m in sorted_metrics[:limit]
        ]

    def _get_recent_failures(self, limit: int = 5) -> List[Dict[str, Any]]:
        """Get recent failed operations"""
        failed_metrics = [m for m in self.metrics if not m.success]
        recent_failures = sorted(failed_metrics, key=lambda m: m.start_time, reverse=True)

        return [
            {
                "operation": m.operation,
                "error": m.error_message,
                "duration": m.duration,
                "timestamp": datetime.fromtimestamp(m.start_time).isoformat()
            }
            for m in recent_failures[:limit]
        ]

    def export_metrics(self, filename: str) -> None:
        """Export metrics to JSON file"""
        metrics_data = {
            "export_timestamp": datetime.now().isoformat(),
            "summary": self.get_metrics(),
            "detailed_metrics": [asdict(m) for m in self.metrics]
        }

        with open(filename, 'w') as f:
            json.dump(metrics_data, f, indent=2, default=str)

        logger.info(f"Performance metrics exported to {filename}")

class AtlanUtils:
    """Utility functions for Atlan operations"""

    def __init__(self, atlan_config: AtlanConfig):
        self.atlan_config = atlan_config
        self.atlan_client = AtlanClient(
            base_url=atlan_config.base_url,
            api_key=atlan_config.api_key
        )
        self.last_run_file = "last_run_timestamp.txt"

    async def get_last_run_timestamp(self) -> datetime:
        """Get the timestamp of the last successful run"""
        try:
            if os.path.exists(self.last_run_file):
                with open(self.last_run_file, 'r') as f:
                    timestamp_str = f.read().strip()
                    return datetime.fromisoformat(timestamp_str)
            else:
                # If no previous run, return a timestamp from 30 days ago
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

    async def search_assets_by_type(self, asset_type: str, limit: int = 100) -> List[Asset]:
        """Search for assets by type"""
        try:
            query = Query(
                where_clause=f"__typeName:{asset_type}",
                size=limit
            )

            search_results = self.atlan_client.asset.search(query)
            return search_results.assets if search_results.assets else []

        except Exception as e:
            logger.error(f"Failed to search assets by type {asset_type}: {str(e)}")
            return []

    async def get_asset_by_guid(self, guid: str) -> Optional[Asset]:
        """Get asset by GUID"""
        try:
            asset = self.atlan_client.asset.get_by_guid(guid)
            return asset

        except Exception as e:
            logger.error(f"Failed to get asset by GUID {guid}: {str(e)}")
            return None

    async def bulk_update_assets(self, assets: List[Asset]) -> Dict[str, Any]:
        """Bulk update multiple assets"""
        try:
            response = self.atlan_client.asset.save(assets)

            return {
                "success": True,
                "updated_count": len(response.updated_assets) if response.updated_assets else 0,
                "created_count": len(response.created_assets) if response.created_assets else 0,
                "failed_count": len(response.failed_assets) if response.failed_assets else 0
            }

        except Exception as e:
            logger.error(f"Failed to bulk update assets: {str(e)}")
            return {
                "success": False,
                "error": str(e),
                "updated_count": 0,
                "created_count": 0,
                "failed_count": len(assets)
            }

    async def validate_connection(self) -> bool:
        """Validate connection to Atlan"""
        try:
            # Try to get current user info to validate connection
            user_info = self.atlan_client.users.get_current_user()
            logger.info(f"Successfully connected to Atlan as user: {user_info.username}")
            return True

        except Exception as e:
            logger.error(f"Failed to validate Atlan connection: {str(e)}")
            return False

    def generate_unique_qualified_name(self, base_name: str, connection_name: str, suffix: str = None) -> str:
        """Generate a unique qualified name for assets"""
        timestamp = int(time.time())

        if suffix:
            return f"default/{connection_name}/{base_name}-{suffix}-{timestamp}"
        else:
            return f"default/{connection_name}/{base_name}-{timestamp}"

    async def cleanup_orphaned_assets(self, asset_type: str, max_age_days: int = 30) -> Dict[str, Any]:
        """Clean up orphaned assets older than specified days"""
        try:
            cutoff_date = datetime.now() - timedelta(days=max_age_days)

            # Search for assets of the specified type
            assets = await self.search_assets_by_type(asset_type)

            orphaned_assets = []
            for asset in assets:
                # Check if asset is orphaned (no lineage, no recent updates)
                if hasattr(asset, 'update_time') and asset.update_time:
                    if asset.update_time < cutoff_date:
                        # Additional checks for orphaned status
                        if await self._is_asset_orphaned(asset):
                            orphaned_assets.append(asset)

            # Delete orphaned assets
            deleted_count = 0
            for asset in orphaned_assets:
                try:
                    self.atlan_client.asset.delete_by_guid(asset.guid)
                    deleted_count += 1
                except Exception as e:
                    logger.error(f"Failed to delete orphaned asset {asset.guid}: {str(e)}")

            return {
                "success": True,
                "total_assets_checked": len(assets),
                "orphaned_assets_found": len(orphaned_assets),
                "deleted_count": deleted_count
            }

        except Exception as e:
            logger.error(f"Failed to cleanup orphaned assets: {str(e)}")
            return {
                "success": False,
                "error": str(e),
                "deleted_count": 0
            }

    async def _is_asset_orphaned(self, asset: Asset) -> bool:
        """Check if an asset is orphaned (no relationships, no recent activity)"""
        try:
            # Check for lineage relationships
            lineage = self.atlan_client.lineage.get_lineage(asset.guid, depth=1)

            if lineage and (lineage.upstream_assets or lineage.downstream_assets):
                return False  # Has relationships, not orphaned

            # Check for recent activity (simplified check)
            if hasattr(asset, 'view_count') and asset.view_count > 0:
                return False  # Has been viewed, likely not orphaned

            return True  # Appears to be orphaned

        except Exception:
            # If we can't determine, err on the side of caution
            return False

class DataQualityValidator:
    """Data quality validation utilities"""

    def __init__(self):
        self.validation_rules = {
            "required_fields": ["name", "qualified_name"],
            "naming_patterns": {
                "s3_object": r"^[a-zA-Z0-9_\-\.]+\.csv$",
                "table": r"^[A-Z_]+$"
            },
            "max_name_length": 255,
            "min_description_length": 10
        }

    def validate_asset_metadata(self, asset_data: Dict[str, Any]) -> Dict[str, Any]:
        """Validate asset metadata quality"""
        validation_results = {
            "is_valid": True,
            "errors": [],
            "warnings": [],
            "quality_score": 100
        }

        # Check required fields
        for field in self.validation_rules["required_fields"]:
            if not asset_data.get(field):
                validation_results["errors"].append(f"Missing required field: {field}")
                validation_results["quality_score"] -= 20

        # Check name length
        name = asset_data.get("name", "")
        if len(name) > self.validation_rules["max_name_length"]:
            validation_results["errors"].append(f"Name too long: {len(name)} > {self.validation_rules['max_name_length']}")
            validation_results["quality_score"] -= 10

        # Check description quality
        description = asset_data.get("description", "")
        if description and len(description) < self.validation_rules["min_description_length"]:
            validation_results["warnings"].append("Description is too short for meaningful context")
            validation_results["quality_score"] -= 5

        # Check naming patterns
        asset_type = asset_data.get("asset_type", "").lower()
        if asset_type in self.validation_rules["naming_patterns"]:
            import re
            pattern = self.validation_rules["naming_patterns"][asset_type]
            if not re.match(pattern, name):
                validation_results["warnings"].append(f"Name doesn't follow expected pattern for {asset_type}")
                validation_results["quality_score"] -= 5

        # Set overall validity
        validation_results["is_valid"] = len(validation_results["errors"]) == 0
        validation_results["quality_score"] = max(0, validation_results["quality_score"])

        return validation_results

    def validate_lineage_relationship(self, relationship: Dict[str, Any]) -> Dict[str, Any]:
        """Validate lineage relationship quality"""
        validation_results = {
            "is_valid": True,
            "errors": [],
            "warnings": [],
            "confidence_score": relationship.get("confidence_score", 0.0)
        }

        # Check required relationship fields
        required_fields = ["source", "target", "relationship_type"]
        for field in required_fields:
            if not relationship.get(field):
                validation_results["errors"].append(f"Missing required relationship field: {field}")

        # Check confidence score
        confidence = relationship.get("confidence_score", 0.0)
        if confidence < 0.5:
            validation_results["warnings"].append(f"Low confidence score: {confidence}")
        elif confidence < 0.7:
            validation_results["warnings"].append(f"Medium confidence score: {confidence}")

        # Validate source and target structure
        for endpoint in ["source", "target"]:
            endpoint_data = relationship.get(endpoint, {})
            if not endpoint_data.get("qualified_name"):
                validation_results["errors"].append(f"Missing qualified_name in {endpoint}")

        validation_results["is_valid"] = len(validation_results["errors"]) == 0

        return validation_results

class ConfigurationManager:
    """Configuration management utilities"""

    def __init__(self, config_file: str = "connector_config.json"):
        self.config_file = config_file
        self.config_cache = {}

    def load_configuration(self) -> Dict[str, Any]:
        """Load configuration from file"""
        try:
            if os.path.exists(self.config_file):
                with open(self.config_file, 'r') as f:
                    self.config_cache = json.load(f)
            else:
                self.config_cache = self._get_default_configuration()
                self.save_configuration()

            return self.config_cache

        except Exception as e:
            logger.error(f"Failed to load configuration: {str(e)}")
            return self._get_default_configuration()

    def save_configuration(self) -> None:
        """Save configuration to file"""
        try:
            with open(self.config_file, 'w') as f:
                json.dump(self.config_cache, f, indent=2)

            logger.info(f"Configuration saved to {self.config_file}")

        except Exception as e:
            logger.error(f"Failed to save configuration: {str(e)}")

    def get_setting(self, key: str, default: Any = None) -> Any:
        """Get a configuration setting"""
        if not self.config_cache:
            self.load_configuration()

        return self.config_cache.get(key, default)

    def set_setting(self, key: str, value: Any) -> None:
        """Set a configuration setting"""
        if not self.config_cache:
            self.load_configuration()

        self.config_cache[key] = value
        self.save_configuration()

    def _get_default_configuration(self) -> Dict[str, Any]:
        """Get default configuration"""
        return {
            "connector_version": "1.0.0",
            "batch_size": 100,
            "retry_attempts": 3,
            "timeout_seconds": 300,
            "enable_caching": True,
            "cache_ttl_hours": 24,
            "log_level": "INFO",
            "performance_monitoring": True,
            "data_quality_validation": True,
            "auto_cleanup_enabled": False,
            "cleanup_max_age_days": 30
        }

class RetryHandler:
    """Retry logic for failed operations"""

    def __init__(self, max_attempts: int = 3, base_delay: float = 1.0, max_delay: float = 60.0):
        self.max_attempts = max_attempts
        self.base_delay = base_delay
        self.max_delay = max_delay

    async def retry_async(self, func, *args, **kwargs):
        """Retry an async function with exponential backoff"""
        last_exception = None

        for attempt in range(self.max_attempts):
            try:
                return await func(*args, **kwargs)

            except Exception as e:
                last_exception = e

                if attempt == self.max_attempts - 1:
                    # Last attempt, re-raise the exception
                    raise e

                # Calculate delay with exponential backoff
                delay = min(self.base_delay * (2 ** attempt), self.max_delay)

                logger.warning(f"Attempt {attempt + 1} failed: {str(e)}. Retrying in {delay} seconds...")
                await asyncio.sleep(delay)

        # This should never be reached, but just in case
        raise last_exception

    def retry_sync(self, func, *args, **kwargs):
        """Retry a synchronous function with exponential backoff"""
        last_exception = None

        for attempt in range(self.max_attempts):
            try:
                return func(*args, **kwargs)

            except Exception as e:
                last_exception = e

                if attempt == self.max_attempts - 1:
                    # Last attempt, re-raise the exception
                    raise e

                # Calculate delay with exponential backoff
                delay = min(self.base_delay * (2 ** attempt), self.max_delay)

                logger.warning(f"Attempt {attempt + 1} failed: {str(e)}. Retrying in {delay} seconds...")
                time.sleep(delay)

        # This should never be reached, but just in case
        raise last_exception

# Utility functions
def format_file_size(size_bytes: int) -> str:
    """Format file size in human-readable format"""
    if size_bytes == 0:
        return "0 B"

    size_names = ["B", "KB", "MB", "GB", "TB"]
    import math
    i = int(math.floor(math.log(size_bytes, 1024)))
    p = math.pow(1024, i)
    s = round(size_bytes / p, 2)

    return f"{s} {size_names[i]}"

def sanitize_name(name: str) -> str:
    """Sanitize name for use in Atlan"""
    import re
    # Remove special characters and replace with underscores
    sanitized = re.sub(r'[^a-zA-Z0-9_\-\.]', '_', name)
    # Remove multiple consecutive underscores
    sanitized = re.sub(r'_+', '_', sanitized)
    # Remove leading/trailing underscores
    sanitized = sanitized.strip('_')

    return sanitized

def generate_asset_hash(asset_data: Dict[str, Any]) -> str:
    """Generate a hash for asset data to detect changes"""
    import hashlib

    # Create a stable string representation of the asset data
    stable_data = {
        "name": asset_data.get("name", ""),
        "qualified_name": asset_data.get("qualified_name", ""),
        "description": asset_data.get("description", ""),
        "schema": str(asset_data.get("schema_info", {}))
    }

    data_string = json.dumps(stable_data, sort_keys=True)
    return hashlib.md5(data_string.encode()).hexdigest()

def validate_qualified_name(qualified_name: str) -> bool:
    """Validate qualified name format"""
    import re

    # Basic pattern: connection/type/path
    pattern = r'^[a-zA-Z0-9_\-]+/[a-zA-Z0-9_\-]+/[a-zA-Z0-9_\-/\.]+$'
    return bool(re.match(pattern, qualified_name))

async def batch_process(items: List[Any], batch_size: int, process_func, *args, **kwargs) -> List[Any]:
    """Process items in batches"""
    results = []

    for i in range(0, len(items), batch_size):
        batch = items[i:i + batch_size]

        try:
            batch_results = await process_func(batch, *args, **kwargs)
            results.extend(batch_results)

        except Exception as e:
            logger.error(f"Failed to process batch {i//batch_size + 1}: {str(e)}")
            # Continue with next batch

    return results

def setup_logging(log_level: str = "INFO", log_file: str = "connector.log") -> None:
    """Setup logging configuration"""
    import logging.handlers

    # Create logger
    logger = logging.getLogger()
    logger.setLevel(getattr(logging, log_level.upper()))

    # Create formatters
    detailed_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'
    )
    simple_formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s'
    )

    # File handler with rotation
    file_handler = logging.handlers.RotatingFileHandler(
        log_file, maxBytes=10*1024*1024, backupCount=5
    )
    file_handler.setFormatter(detailed_formatter)
    logger.addHandler(file_handler)

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(simple_formatter)
    logger.addHandler(console_handler)

    logger.info(f"Logging configured with level: {log_level}")
