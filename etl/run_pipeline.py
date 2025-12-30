#!/usr/bin/env python3
"""
Mini Data Engineering Project — Orders Revenue Pipeline
========================================================

This pipeline processes raw orders and order items CSV files to compute
daily revenue with comprehensive data quality validation.

Author: Senior Data Engineer
"""

import argparse
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)


class DataQualityReport:
    """Tracks data quality metrics throughout pipeline execution."""
    
    def __init__(self, run_date: str):
        self.run_date = run_date
        self.pipeline_start_time = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        self.pipeline_end_time: str | None = None
        self.input_counts = {"orders": 0, "order_items": 0}
        self.duplicate_counts = {"orders": 0, "order_items": 0}
        self.rejected_counts = {"orders": 0, "order_items": 0}
        self.orphan_items_count = 0
        self.valid_counts = {"orders": 0, "order_items": 0}
        self.rejection_reasons: dict[str, list[dict[str, Any]]] = {
            "orders": [],
            "order_items": []
        }
        self.output_metrics = {
            "daily_revenue_rows": 0,
            "total_revenue": 0.0,
            "total_orders_count": 0
        }
    
    def to_dict(self) -> dict:
        """Convert report to dictionary for JSON serialization."""
        return {
            "run_date": self.run_date,
            "pipeline_start_time": self.pipeline_start_time,
            "pipeline_end_time": self.pipeline_end_time,
            "input": self.input_counts,
            "duplicates": self.duplicate_counts,
            "rejected": self.rejected_counts,
            "orphan_items": self.orphan_items_count,
            "valid": self.valid_counts,
            "rejection_reasons": {
                "orders": self._summarize_reasons(self.rejection_reasons["orders"]),
                "order_items": self._summarize_reasons(self.rejection_reasons["order_items"])
            },
            "output": self.output_metrics
        }
    
    def _summarize_reasons(self, reasons: list[dict]) -> dict:
        """Summarize rejection reasons by type."""
        summary: dict[str, int] = {}
        for item in reasons:
            reason = item.get("reason", "unknown")
            summary[reason] = summary.get(reason, 0) + 1
        return summary


def load_csv_with_validation(filepath: Path, expected_columns: list[str]) -> pd.DataFrame:
    """
    Load CSV file with basic validation.
    
    Args:
        filepath: Path to CSV file
        expected_columns: List of expected column names
        
    Returns:
        DataFrame with loaded data
        
    Raises:
        FileNotFoundError: If file doesn't exist
        ValueError: If required columns are missing
    """
    if not filepath.exists():
        raise FileNotFoundError(f"Missing input file: {filepath}")
    
    logger.info(f"Loading file: {filepath}")
    df = pd.read_csv(filepath, dtype=str)  # Load as string initially for cleaning
    
    # Normalize column names: lowercase, strip whitespace
    df.columns = df.columns.str.lower().str.strip()
    
    # Check for required columns
    missing_cols = set(expected_columns) - set(df.columns)
    if missing_cols:
        raise ValueError(f"Missing required columns in {filepath.name}: {missing_cols}")
    
    logger.info(f"Loaded {len(df)} rows from {filepath.name}")
    return df


def clean_and_standardize_orders(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and standardize orders DataFrame.
    
    Operations:
    - Trim whitespace from all string columns
    - Standardize status values (lowercase, trimmed)
    - Parse datetime fields
    - Cast appropriate data types
    """
    df = df.copy()
    
    # Trim whitespace from all columns
    for col in df.columns:
        if df[col].dtype == object:
            df[col] = df[col].str.strip()
    
    # Replace empty strings with NaN for proper null handling
    df = df.replace(r'^\s*$', pd.NA, regex=True)
    df = df.replace('', pd.NA)
    
    # Standardize status: lowercase
    if 'status' in df.columns:
        df['status'] = df['status'].str.lower().str.strip()
    
    # Parse datetime fields
    if 'order_date' in df.columns:
        df['order_date'] = pd.to_datetime(df['order_date'], errors='coerce').dt.date
    
    if 'ingested_at' in df.columns:
        df['ingested_at'] = pd.to_datetime(df['ingested_at'], errors='coerce')
    
    return df


def clean_and_standardize_items(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and standardize order items DataFrame.
    
    Operations:
    - Trim whitespace from all string columns
    - Parse datetime fields
    - Cast numeric types
    """
    df = df.copy()
    
    # Trim whitespace from all columns
    for col in df.columns:
        if df[col].dtype == object:
            df[col] = df[col].str.strip()
    
    # Replace empty strings with NaN
    df = df.replace(r'^\s*$', pd.NA, regex=True)
    df = df.replace('', pd.NA)
    
    # Parse datetime fields
    if 'ingested_at' in df.columns:
        df['ingested_at'] = pd.to_datetime(df['ingested_at'], errors='coerce')
    
    # Cast numeric types
    if 'quantity' in df.columns:
        df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce')
    
    if 'unit_price' in df.columns:
        df['unit_price'] = pd.to_numeric(df['unit_price'], errors='coerce')
    
    return df


def deduplicate_orders(df: pd.DataFrame, report: DataQualityReport) -> pd.DataFrame:
    """
    Deduplicate orders by order_id, keeping the row with latest ingested_at.
    
    This ensures deterministic deduplication for idempotent reruns.
    """
    if df.empty:
        return df
    
    original_count = len(df)
    
    # Sort by order_id and ingested_at (descending) to keep latest
    df_sorted = df.sort_values(
        by=['order_id', 'ingested_at'],
        ascending=[True, False],
        na_position='last'
    )
    
    # Keep first occurrence (latest ingested_at) for each order_id
    df_dedup = df_sorted.drop_duplicates(subset=['order_id'], keep='first')
    
    duplicates_removed = original_count - len(df_dedup)
    report.duplicate_counts["orders"] = duplicates_removed
    
    if duplicates_removed > 0:
        logger.info(f"Removed {duplicates_removed} duplicate order(s)")
    
    return df_dedup.reset_index(drop=True)


def validate_orders(
    df: pd.DataFrame,
    report: DataQualityReport
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Validate orders against business rules.
    
    Rules:
    - order_id must be NOT NULL
    - customer_id must be NOT NULL
    - order_date must be NOT NULL
    - status must be NOT NULL
    
    Returns:
        Tuple of (valid_orders, rejected_orders)
    """
    if df.empty:
        return df, pd.DataFrame()
    
    df = df.copy()
    df['_rejection_reason'] = None
    
    # Check each required field
    required_fields = ['order_id', 'customer_id', 'order_date', 'status']
    
    for field in required_fields:
        mask = df[field].isna()
        df.loc[mask & df['_rejection_reason'].isna(), '_rejection_reason'] = f"null_{field}"
    
    # Split valid and rejected
    rejected_mask = df['_rejection_reason'].notna()
    rejected_df = df[rejected_mask].copy()
    valid_df = df[~rejected_mask].copy()
    
    # Record rejection reasons in report
    for _, row in rejected_df.iterrows():
        report.rejection_reasons["orders"].append({
            "order_id": row.get('order_id'),
            "reason": row['_rejection_reason']
        })
    
    # Clean up helper column
    valid_df = valid_df.drop(columns=['_rejection_reason'])
    rejected_df = rejected_df.rename(columns={'_rejection_reason': 'rejection_reason'})
    
    report.rejected_counts["orders"] = len(rejected_df)
    report.valid_counts["orders"] = len(valid_df)
    
    logger.info(f"Orders validation: {len(valid_df)} valid, {len(rejected_df)} rejected")
    
    return valid_df, rejected_df


def validate_items(
    df: pd.DataFrame,
    valid_order_ids: set,
    report: DataQualityReport
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Validate order items against business rules.
    
    Rules:
    - quantity must be NOT NULL
    - unit_price must be > 0
    - order_id must exist in valid orders (orphan check)
    
    Returns:
        Tuple of (valid_items, rejected_items)
    """
    if df.empty:
        return df, pd.DataFrame()
    
    df = df.copy()
    df['_rejection_reason'] = None
    
    # Rule: quantity must be NOT NULL
    mask_null_qty = df['quantity'].isna()
    df.loc[mask_null_qty & df['_rejection_reason'].isna(), '_rejection_reason'] = "null_quantity"
    
    # Rule: unit_price must be > 0
    mask_invalid_price = (df['unit_price'].isna()) | (df['unit_price'] <= 0)
    df.loc[mask_invalid_price & df['_rejection_reason'].isna(), '_rejection_reason'] = "invalid_unit_price"
    
    # Rule: orphan items (order_id not in valid orders)
    mask_orphan = ~df['order_id'].isin(valid_order_ids)
    orphan_count = (mask_orphan & df['_rejection_reason'].isna()).sum()
    df.loc[mask_orphan & df['_rejection_reason'].isna(), '_rejection_reason'] = "orphan_item"
    report.orphan_items_count = int(orphan_count)
    
    # Split valid and rejected
    rejected_mask = df['_rejection_reason'].notna()
    rejected_df = df[rejected_mask].copy()
    valid_df = df[~rejected_mask].copy()
    
    # Record rejection reasons in report
    for _, row in rejected_df.iterrows():
        report.rejection_reasons["order_items"].append({
            "item_id": row.get('item_id'),
            "order_id": row.get('order_id'),
            "reason": row['_rejection_reason']
        })
    
    # Clean up helper column
    valid_df = valid_df.drop(columns=['_rejection_reason'])
    rejected_df = rejected_df.rename(columns={'_rejection_reason': 'rejection_reason'})
    
    report.rejected_counts["order_items"] = len(rejected_df)
    report.valid_counts["order_items"] = len(valid_df)
    
    logger.info(f"Items validation: {len(valid_df)} valid, {len(rejected_df)} rejected")
    
    return valid_df, rejected_df


def compute_daily_revenue(
    orders_df: pd.DataFrame,
    items_df: pd.DataFrame,
    report: DataQualityReport
) -> pd.DataFrame:
    """
    Compute daily revenue from completed orders.
    
    Business Logic:
    - Only orders with status = 'completed' count toward revenue
    - Revenue = quantity * unit_price for each item
    - Aggregate by order_date
    
    Returns:
        DataFrame with columns: order_date, total_revenue, orders_count
    """
    if orders_df.empty or items_df.empty:
        logger.warning("Empty orders or items - returning empty revenue DataFrame")
        return pd.DataFrame(columns=['order_date', 'total_revenue', 'orders_count'])
    
    # Filter for completed orders only
    completed_orders = orders_df[orders_df['status'] == 'completed'].copy()
    
    if completed_orders.empty:
        logger.warning("No completed orders found")
        return pd.DataFrame(columns=['order_date', 'total_revenue', 'orders_count'])
    
    logger.info(f"Processing {len(completed_orders)} completed order(s)")
    
    # Join items with completed orders
    merged = items_df.merge(
        completed_orders[['order_id', 'order_date']],
        on='order_id',
        how='inner'
    )
    
    if merged.empty:
        logger.warning("No items found for completed orders")
        return pd.DataFrame(columns=['order_date', 'total_revenue', 'orders_count'])
    
    # Calculate item amount
    merged['amount'] = merged['quantity'] * merged['unit_price']
    
    # Aggregate by order_date
    # First, get revenue per order
    order_revenue = merged.groupby(['order_date', 'order_id'])['amount'].sum().reset_index()
    
    # Then aggregate by date
    daily_revenue = order_revenue.groupby('order_date').agg(
        total_revenue=('amount', 'sum'),
        orders_count=('order_id', 'nunique')
    ).reset_index()
    
    # Sort by date for consistent output
    daily_revenue = daily_revenue.sort_values('order_date').reset_index(drop=True)
    
    # Round revenue to 2 decimal places
    daily_revenue['total_revenue'] = daily_revenue['total_revenue'].round(2)
    
    # Update report metrics
    report.output_metrics["daily_revenue_rows"] = len(daily_revenue)
    report.output_metrics["total_revenue"] = float(daily_revenue['total_revenue'].sum())
    report.output_metrics["total_orders_count"] = int(daily_revenue['orders_count'].sum())
    
    logger.info(
        f"Computed daily revenue: {len(daily_revenue)} day(s), "
        f"${report.output_metrics['total_revenue']:.2f} total revenue, "
        f"{report.output_metrics['total_orders_count']} order(s)"
    )
    
    return daily_revenue


def write_outputs(
    output_dir: Path,
    daily_revenue: pd.DataFrame,
    rejected_orders: pd.DataFrame,
    rejected_items: pd.DataFrame,
    report: DataQualityReport
) -> None:
    """
    Write all output files.
    
    Files are overwritten to ensure idempotency.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Write daily revenue
    revenue_path = output_dir / "daily_revenue.csv"
    daily_revenue.to_csv(revenue_path, index=False)
    logger.info(f"Wrote {len(daily_revenue)} row(s) to {revenue_path.name}")
    
    # Write rejected orders (if any)
    if not rejected_orders.empty:
        rejected_orders_path = output_dir / "rejected_orders.csv"
        rejected_orders.to_csv(rejected_orders_path, index=False)
        logger.info(f"Wrote {len(rejected_orders)} rejected order(s) to {rejected_orders_path.name}")
    
    # Write rejected items (if any)
    if not rejected_items.empty:
        rejected_items_path = output_dir / "rejected_items.csv"
        rejected_items.to_csv(rejected_items_path, index=False)
        logger.info(f"Wrote {len(rejected_items)} rejected item(s) to {rejected_items_path.name}")
    
    # Finalize and write quality report
    report.pipeline_end_time = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    report_path = output_dir / "quality_report.json"
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report.to_dict(), f, indent=2, default=str)
    logger.info(f"Wrote quality report to {report_path.name}")


def main(run_date: str, input_dir: str, output_dir: str) -> None:
    """
    Main pipeline orchestration function.
    
    Args:
        run_date: Date string in YYYY-MM-DD format
        input_dir: Directory containing input CSV files
        output_dir: Directory for output files
    """
    logger.info(f"=" * 60)
    logger.info(f"Starting pipeline for run_date: {run_date}")
    logger.info(f"Input directory: {input_dir}")
    logger.info(f"Output directory: {output_dir}")
    logger.info(f"=" * 60)
    
    # Initialize quality report
    report = DataQualityReport(run_date)
    
    input_path = Path(input_dir)
    output_path = Path(output_dir)
    
    # Define file paths
    orders_file = input_path / f"orders_{run_date}.csv"
    items_file = input_path / f"order_items_{run_date}.csv"
    
    # === STAGE 1: Load Input Data ===
    logger.info("STAGE 1: Loading input data...")
    
    orders_expected_cols = ['order_id', 'customer_id', 'order_date', 'status', 'ingested_at']
    items_expected_cols = ['order_id', 'quantity', 'unit_price', 'ingested_at']
    
    orders_raw = load_csv_with_validation(orders_file, orders_expected_cols)
    items_raw = load_csv_with_validation(items_file, items_expected_cols)
    
    report.input_counts["orders"] = len(orders_raw)
    report.input_counts["order_items"] = len(items_raw)
    
    # === STAGE 2: Clean and Standardize ===
    logger.info("STAGE 2: Cleaning and standardizing data...")
    
    orders_clean = clean_and_standardize_orders(orders_raw)
    items_clean = clean_and_standardize_items(items_raw)
    
    # === STAGE 3: Deduplicate Orders ===
    logger.info("STAGE 3: Deduplicating orders...")
    
    orders_dedup = deduplicate_orders(orders_clean, report)
    
    # === STAGE 4: Validate Data ===
    logger.info("STAGE 4: Validating data quality...")
    
    valid_orders, rejected_orders = validate_orders(orders_dedup, report)
    
    # Get set of valid order IDs for orphan check
    valid_order_ids = set(valid_orders['order_id'].dropna().unique())
    
    valid_items, rejected_items = validate_items(items_clean, valid_order_ids, report)
    
    # === STAGE 5: Compute Daily Revenue ===
    logger.info("STAGE 5: Computing daily revenue...")
    
    daily_revenue = compute_daily_revenue(valid_orders, valid_items, report)
    
    # === STAGE 6: Write Outputs ===
    logger.info("STAGE 6: Writing output files...")
    
    write_outputs(output_path, daily_revenue, rejected_orders, rejected_items, report)
    
    # === Pipeline Complete ===
    logger.info("=" * 60)
    logger.info("Pipeline completed successfully!")
    logger.info(f"Input: {report.input_counts['orders']} orders, {report.input_counts['order_items']} items")
    logger.info(f"Valid: {report.valid_counts['orders']} orders, {report.valid_counts['order_items']} items")
    logger.info(f"Rejected: {report.rejected_counts['orders']} orders, {report.rejected_counts['order_items']} items")
    logger.info(f"Total Revenue: ${report.output_metrics['total_revenue']:.2f}")
    logger.info("=" * 60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Mini Data Engineering Pipeline - Daily Revenue Computation"
    )
    parser.add_argument(
        "--run-date",
        required=True,
        help="Run date in YYYY-MM-DD format (e.g., 2024-01-01)"
    )
    parser.add_argument(
        "--input-dir",
        default="data",
        help="Directory containing input CSV files (default: data)"
    )
    parser.add_argument(
        "--output-dir",
        default="output",
        help="Directory for output files (default: output)"
    )
    args = parser.parse_args()
    
    # Validate date format
    try:
        datetime.strptime(args.run_date, "%Y-%m-%d")
    except ValueError:
        raise ValueError(f"Invalid date format: {args.run_date}. Use YYYY-MM-DD format.")
    
    main(args.run_date, args.input_dir, args.output_dir)
