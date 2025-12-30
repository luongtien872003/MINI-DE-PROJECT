# Mini Data Engineering Project — Orders Revenue Pipeline

## 1. Overview

This project implements a **batch data pipeline** that computes **daily revenue** from raw CSV files containing orders and order items data. The pipeline follows data engineering best practices including:

- Data cleaning and standardization
- Comprehensive data quality validation
- Deterministic deduplication
- Referential integrity checks
- Idempotent and reproducible outputs

---

## 2. Quick Start

### 2.1 Prerequisites

- Python 3.10+ installed
- pip (Python package manager)

### 2.2 Installation (Windows PowerShell)

```powershell
# Navigate to project directory
cd mini-de-project

# Create virtual environment
python -m venv .venv

# Activate virtual environment
.\.venv\Scripts\Activate.ps1

# Install dependencies
pip install -r requirements.txt
```

### 2.3 Running the Pipeline

```powershell
python etl\run_pipeline.py --run-date 2024-01-01 --input-dir data --output-dir output
```

### 2.4 Installation (Linux/macOS)

```bash
cd mini-de-project
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python etl/run_pipeline.py --run-date 2024-01-01 --input-dir data --output-dir output
```

---

## 3. Input Data

### 3.1 File Locations

Input files are expected in the `data/` directory with the following naming convention:
- `orders_{YYYY-MM-DD}.csv`
- `order_items_{YYYY-MM-DD}.csv`

### 3.2 Orders Schema

| Column | Type | Description |
|--------|------|-------------|
| order_id | string | Unique order identifier |
| customer_id | string | Customer identifier |
| order_date | date | Date of the order (YYYY-MM-DD) |
| status | string | Order status (e.g., completed, pending, cancelled) |
| ingested_at | timestamp | When the record was ingested (ISO 8601) |

### 3.3 Order Items Schema

| Column | Type | Description |
|--------|------|-------------|
| item_id | string | Unique item identifier |
| order_id | string | Reference to parent order |
| product_id | string | Product identifier |
| quantity | numeric | Quantity ordered |
| unit_price | numeric | Price per unit |
| ingested_at | timestamp | When the record was ingested (ISO 8601) |

---

## 4. Processing Logic

### 4.1 Pipeline Stages

```
┌─────────────────┐
│  STAGE 1: Load  │  Read CSV files with basic validation
└────────┬────────┘
         │
┌────────▼────────┐
│ STAGE 2: Clean  │  Standardize formats, trim whitespace
└────────┬────────┘
         │
┌────────▼────────┐
│ STAGE 3: Dedup  │  Remove duplicates, keep latest
└────────┬────────┘
         │
┌────────▼────────┐
│STAGE 4: Validate│  Apply DQ rules, separate rejects
└────────┬────────┘
         │
┌────────▼────────┐
│STAGE 5: Compute │  Calculate daily revenue
└────────┬────────┘
         │
┌────────▼────────┐
│ STAGE 6: Output │  Write results and reports
└─────────────────┘
```

### 4.2 Data Cleaning & Standardization

**Orders:**
- Column names normalized to lowercase
- Whitespace trimmed from all string fields
- `status` standardized to lowercase
- `order_date` parsed to date type
- `ingested_at` parsed to timestamp type

**Order Items:**
- Column names normalized to lowercase
- Whitespace trimmed from all string fields
- `quantity` and `unit_price` cast to numeric types
- `ingested_at` parsed to timestamp type

### 4.3 Deduplication Rules

**Orders:** Deduplicated by `order_id`, keeping the row with the **latest** `ingested_at` timestamp. This ensures deterministic, repeatable results.

### 4.4 Data Quality Rules

**Orders (all must be NOT NULL):**
| Field | Rule | Rejection Code |
|-------|------|----------------|
| order_id | NOT NULL | `null_order_id` |
| customer_id | NOT NULL | `null_customer_id` |
| order_date | NOT NULL | `null_order_date` |
| status | NOT NULL | `null_status` |

**Order Items:**
| Field | Rule | Rejection Code |
|-------|------|----------------|
| quantity | NOT NULL | `null_quantity` |
| unit_price | > 0 | `invalid_unit_price` |
| order_id | Must exist in valid orders | `orphan_item` |

### 4.5 Revenue Calculation

**Business Rules:**
- Only orders with `status = 'completed'` count toward revenue
- Item amount = `quantity × unit_price`
- Revenue is aggregated by `order_date`

**Output Columns:**
- `order_date`: The date of the orders
- `total_revenue`: Sum of (quantity × unit_price) for all items
- `orders_count`: Number of distinct orders

---

## 5. Output Files

All outputs are written to the `output/` directory:

| File | Description |
|------|-------------|
| `daily_revenue.csv` | Aggregated daily revenue (BI-ready) |
| `rejected_orders.csv` | Orders that failed validation (if any) |
| `rejected_items.csv` | Order items that failed validation (if any) |
| `quality_report.json` | Comprehensive data quality metrics |

### 5.1 Daily Revenue Format

```csv
order_date,total_revenue,orders_count
2024-01-01,100.0,3
2024-01-02,50.0,1
```

### 5.2 Quality Report Structure

```json
{
  "run_date": "2024-01-01",
  "pipeline_start_time": "2025-12-30T10:20:24.602864Z",
  "pipeline_end_time": "2025-12-30T10:20:24.640980Z",
  "input": {
    "orders": 11,
    "order_items": 11
  },
  "duplicates": {
    "orders": 1,
    "order_items": 0
  },
  "rejected": {
    "orders": 4,
    "order_items": 3
  },
  "orphan_items": 1,
  "valid": {
    "orders": 6,
    "order_items": 8
  },
  "rejection_reasons": {
    "orders": {
      "null_customer_id": 1,
      "null_order_date": 1,
      "null_status": 1,
      "null_order_id": 1
    },
    "order_items": {
      "invalid_unit_price": 1,
      "orphan_item": 1,
      "null_quantity": 1
    }
  },
  "output": {
    "daily_revenue_rows": 2,
    "total_revenue": 150.0,
    "total_orders_count": 4
  }
}
```

---

## 6. Idempotency & Safe Re-runs

The pipeline is designed to be **idempotent**:

- ✅ Running multiple times produces identical outputs
- ✅ Output files are overwritten (not appended)
- ✅ Deduplication is deterministic (latest `ingested_at` wins)
- ✅ No side effects or state accumulation

---

## 7. Assumptions

1. **File Naming:** Input files follow the pattern `{type}_{date}.csv`
2. **Date Format:** All dates are in ISO 8601 format (YYYY-MM-DD)
3. **Timestamp Format:** All timestamps are in ISO 8601 format with timezone
4. **Currency:** Unit prices and revenues are in the same currency (no conversion)
5. **Deduplication:** For duplicate order_ids, the record with the most recent `ingested_at` is the correct one
6. **Orphan Items:** Items referencing non-existent orders are invalid data
7. **Zero Quantity:** Items with quantity = 0 are valid but contribute $0 to revenue
8. **Status Values:** Only `completed` status counts toward revenue

---

## 8. Project Structure

```
mini-de-project/
├── README.md                    # This documentation
├── requirements.txt             # Python dependencies
├── data/                        # Input CSV files
│   ├── orders_2024-01-01.csv
│   └── order_items_2024-01-01.csv
├── etl/                         # Pipeline code
│   └── run_pipeline.py          # Main pipeline script
├── sql/                         # SQL reference models
│   ├── models/
│   │   ├── stg_orders.sql       # Staging orders
│   │   ├── stg_order_items.sql  # Staging items
│   │   └── mart_daily_revenue.sql  # Revenue mart
│   └── checks/
│       └── dq_checks.sql        # Data quality checks
└── output/                      # Generated output files
    ├── daily_revenue.csv
    ├── rejected_orders.csv
    ├── rejected_items.csv
    └── quality_report.json
```

---

## 9. Error Handling

| Error Type | Handling |
|------------|----------|
| Missing input file | Pipeline fails with clear error message |
| Missing required columns | Pipeline fails with column list |
| Invalid date format | Row marked as null, may be rejected |
| Invalid numeric values | Coerced to null, validated by DQ rules |
| Orphan items | Rejected and logged in quality report |

---

## 10. Monitoring & Observability

The `quality_report.json` provides metrics for monitoring:

- **Input counts:** Track data volume trends
- **Duplicate counts:** Detect upstream issues
- **Rejection counts:** Monitor data quality
- **Rejection reasons:** Identify specific data issues
- **Output metrics:** Verify business results

---

## 11. Command Line Options

```
python etl/run_pipeline.py --help

usage: run_pipeline.py [-h] --run-date RUN_DATE [--input-dir INPUT_DIR] [--output-dir OUTPUT_DIR]

Mini Data Engineering Pipeline - Daily Revenue Computation

options:
  -h, --help            show this help message and exit
  --run-date RUN_DATE   Run date in YYYY-MM-DD format (e.g., 2024-01-01)
  --input-dir INPUT_DIR
                        Directory containing input CSV files (default: data)
  --output-dir OUTPUT_DIR
                        Directory for output files (default: output)
```

---

## 12. Testing

To verify the pipeline is working correctly:

```bash
# Run the pipeline
python etl/run_pipeline.py --run-date 2024-01-01 --input-dir data --output-dir output

# Verify outputs exist
ls output/

# Check daily revenue
cat output/daily_revenue.csv

# Review quality report
cat output/quality_report.json

# Run again to verify idempotency
python etl/run_pipeline.py --run-date 2024-01-01 --input-dir data --output-dir output
# Outputs should be identical
```

---

## 13. Sample Data Issues (for testing)

The sample data includes these intentional data quality issues:

| Issue Type | Example |
|------------|---------|
| Duplicate order | Order 1001 appears twice with different timestamps |
| Null customer_id | Order 1005 has no customer |
| Null order_date | Order 1006 has no date |
| Null status | Order 1007 has no status |
| Null order_id | One order has no ID |
| Negative unit_price | Item I004 has price = -5.00 |
| Null quantity | Item I009 has no quantity |
| Orphan item | Item I008 references order 9999 (doesn't exist) |
| Non-completed status | Order 1003 (pending), Order 1010 (cancelled) |

---

## 14. Future Enhancements

Potential improvements for production deployment:

- [ ] Add support for partitioned output (by date)
- [ ] Implement incremental processing
- [ ] Add database sink (PostgreSQL, BigQuery, etc.)
- [ ] Add alerting on DQ threshold violations
- [ ] Implement data lineage tracking
- [ ] Add unit tests and integration tests
- [ ] Containerize with Docker
- [ ] Add CI/CD pipeline configuration

---

## 15. License

This project is for educational and demonstration purposes.
