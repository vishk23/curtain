"""
Oracle Table Diagnostics - WH_ACCTCOMMON, WH_ACCT, WH_LOANS

Run this against Oracle to identify:
1. Date columns with values outside .NET range (< 0001-01-01 or > 9999-12-31)
2. Number/Decimal precision issues
3. Sample problematic rows

Usage: Run in any Python environment with Oracle connection (cx_Oracle, oracledb, or via Spark)
"""

# ============================================================
# CONFIGURATION
# ============================================================

TABLES_TO_CHECK = {
    "WH_LOANS": {
        "date_filter_col": "RUNDATE",  # Use for filtering snapshot
        "date_columns": ["RUNDATE", "ORIGDATE", "DATEMAT", "PAIDOFFDATE", "DATELASTMAINT", "ADDDATE", "STOPDATE"],
        "numeric_columns": ["ORIGBAL", "NOTEBAL", "BOOKBALANCE", "AVAILBALAMT", "COBAL", "PCTPARTSOLD", "LCRATE", "OLDPI", "PF"],
    },
    "WH_ACCTCOMMON": {
        "date_filter_col": "EFFDATE",
        "date_columns": ["EFFDATE", "CONTRACTDATE", "DATEMAT", "CLOSEDATE", "DATELASTMAINT"],
        "numeric_columns": ["BOOKBALANCE", "AVAILBAL", "YTDINTPD", "YTDINTACC", "INTRATE"],
    },
    "WH_ACCT": {
        "date_filter_col": "RUNDATE",
        "date_columns": ["RUNDATE", "DATEMAT", "EFFDATE", "DATELASTMAINT"],
        "numeric_columns": ["BOOKBALANCE", "AVAILBAL", "INTRATE"],
    },
}

SCHEMA = "COCCDM"

# ============================================================
# DIAGNOSTIC QUERIES - Run these in SQL Developer or similar
# ============================================================

print("=" * 70)
print("DIAGNOSTIC SQL QUERIES")
print("Run these in SQL Developer, TOAD, or any Oracle client")
print("=" * 70)

# 1. Check for dates outside valid range
print("\n" + "-" * 70)
print("1. INVALID DATE CHECK")
print("   Dates < 0001-01-01 or > 9999-12-31 cause .NET DateTime errors")
print("-" * 70)

for table, config in TABLES_TO_CHECK.items():
    print(f"\n-- {table} date validation")
    for date_col in config["date_columns"]:
        print(f"""
SELECT '{table}' as tbl, '{date_col}' as col,
       COUNT(*) as invalid_count,
       MIN({date_col}) as min_date,
       MAX({date_col}) as max_date
FROM {SCHEMA}.{table}
WHERE {date_col} < TO_DATE('0001-01-01', 'YYYY-MM-DD')
   OR {date_col} > TO_DATE('9999-12-31', 'YYYY-MM-DD')
   OR EXTRACT(YEAR FROM {date_col}) < 1
   OR EXTRACT(YEAR FROM {date_col}) > 9999;
""")

# 2. Check for NULL vs invalid dates
print("\n" + "-" * 70)
print("2. DATE DISTRIBUTION CHECK")
print("   Understand what dates are actually in the data")
print("-" * 70)

for table, config in TABLES_TO_CHECK.items():
    date_col = config["date_filter_col"]
    print(f"""
-- {table}: Date range and distribution
SELECT
    MIN({date_col}) as min_date,
    MAX({date_col}) as max_date,
    COUNT(*) as total_rows,
    COUNT({date_col}) as non_null_dates,
    COUNT(*) - COUNT({date_col}) as null_dates
FROM {SCHEMA}.{table};

-- Recent dates available
SELECT {date_col}, COUNT(*) as row_count
FROM {SCHEMA}.{table}
WHERE {date_col} >= TRUNC(SYSDATE) - 7
GROUP BY {date_col}
ORDER BY {date_col} DESC;
""")

# 3. Check for problematic numeric precision
print("\n" + "-" * 70)
print("3. NUMERIC PRECISION CHECK")
print("   Check for NUMBER columns that exceed DECIMAL(38,18) precision")
print("-" * 70)

for table, config in TABLES_TO_CHECK.items():
    print(f"\n-- {table} numeric precision")
    print(f"""
SELECT column_name, data_type, data_precision, data_scale
FROM all_tab_columns
WHERE owner = '{SCHEMA}'
  AND table_name = '{table}'
  AND data_type = 'NUMBER'
ORDER BY column_id;
""")

# 4. Check for extreme numeric values
print("\n" + "-" * 70)
print("4. EXTREME NUMERIC VALUES")
print("   Values that might overflow Spark Decimal types")
print("-" * 70)

for table, config in TABLES_TO_CHECK.items():
    for num_col in config["numeric_columns"]:
        print(f"""
-- {table}.{num_col} range check
SELECT '{table}' as tbl, '{num_col}' as col,
       MIN({num_col}) as min_val,
       MAX({num_col}) as max_val,
       MAX(LENGTH(TRUNC({num_col}))) as max_int_digits,
       MAX(LENGTH({num_col}) - LENGTH(TRUNC({num_col})) - 1) as max_dec_digits
FROM {SCHEMA}.{table}
WHERE {num_col} IS NOT NULL;
""")

# 5. Sample problematic rows
print("\n" + "-" * 70)
print("5. SAMPLE PROBLEMATIC ROWS")
print("   Get actual examples of bad data")
print("-" * 70)

print("""
-- WH_LOANS: Find rows with suspicious dates
SELECT ACCTNBR, RUNDATE, ORIGDATE, DATEMAT, PAIDOFFDATE, STATUS
FROM COCCDM.WH_LOANS
WHERE ORIGDATE < TO_DATE('1900-01-01', 'YYYY-MM-DD')
   OR DATEMAT > TO_DATE('2100-12-31', 'YYYY-MM-DD')
   OR EXTRACT(YEAR FROM ORIGDATE) < 1900
   OR EXTRACT(YEAR FROM DATEMAT) > 2100
FETCH FIRST 20 ROWS ONLY;

-- Check for DATE columns that are actually storing weird values
SELECT ACCTNBR, RUNDATE,
       TO_CHAR(ORIGDATE, 'YYYY-MM-DD HH24:MI:SS') as origdate_str,
       TO_CHAR(DATEMAT, 'YYYY-MM-DD HH24:MI:SS') as datemat_str
FROM COCCDM.WH_LOANS
WHERE ORIGDATE IS NOT NULL
ORDER BY ORIGDATE ASC
FETCH FIRST 10 ROWS ONLY;
""")

# 6. Quick counts
print("\n" + "-" * 70)
print("6. QUICK ROW COUNTS")
print("-" * 70)

print("""
SELECT 'WH_LOANS' as tbl, COUNT(*) as cnt FROM COCCDM.WH_LOANS
UNION ALL
SELECT 'WH_ACCTCOMMON', COUNT(*) FROM COCCDM.WH_ACCTCOMMON
UNION ALL
SELECT 'WH_ACCT', COUNT(*) FROM COCCDM.WH_ACCT;
""")

# ============================================================
# RECOMMENDED COPYJOB QUERIES
# ============================================================

print("\n" + "=" * 70)
print("RECOMMENDED COPYJOB SAFE QUERIES")
print("Use these queries in CopyJob to handle DateTime issues")
print("=" * 70)

print("""
-- WH_LOANS: Cast problematic dates to NULL
SELECT
    ACCTNBR, RUNDATE, OCC, STATUS, ORIGBAL, CURRTERM, INTC, LCRATE, OLDPI,
    CASE
        WHEN ORIGDATE < TO_DATE('1900-01-01', 'YYYY-MM-DD')
          OR ORIGDATE > TO_DATE('2100-12-31', 'YYYY-MM-DD')
        THEN NULL
        ELSE ORIGDATE
    END as ORIGDATE,
    CASE
        WHEN DATEMAT < TO_DATE('1900-01-01', 'YYYY-MM-DD')
          OR DATEMAT > TO_DATE('2100-12-31', 'YYYY-MM-DD')
        THEN NULL
        ELSE DATEMAT
    END as DATEMAT,
    CASE
        WHEN PAIDOFFDATE < TO_DATE('1900-01-01', 'YYYY-MM-DD')
          OR PAIDOFFDATE > TO_DATE('2100-12-31', 'YYYY-MM-DD')
        THEN NULL
        ELSE PAIDOFFDATE
    END as PAIDOFFDATE,
    PF, NOTEBAL, BOOKBALANCE, AVAILBALAMT, COBAL, ACCTMISC3, PCTPARTSOLD,
    DATELASTMAINT, ADDDATE, STOPDATE
FROM COCCDM.WH_LOANS
WHERE RUNDATE = TRUNC(SYSDATE) - 1;  -- Yesterday's snapshot
""")

print("\n" + "=" * 70)
print("END OF DIAGNOSTIC SCRIPT")
print("=" * 70)
