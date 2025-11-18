# 📊 Check Firebolt Column Data Types - Jupyter Notebook Guide

## 🎯 Purpose

Simple Python code to check data types for **all columns** in **all tables** in your Firebolt database.

**Compatible with:** Python 3.13.9 (and 3.9+)

---

## 🚀 Quick Start (Copy-Paste Into Jupyter)

### **Cell 1: Install Packages**

```python
# Run this ONCE
!pip install firebolt-sdk pandas openpyxl
```

---

### **Cell 2: Import & Configure**

```python
from firebolt.db import connect
from firebolt.client.auth import ClientCredentials
import pandas as pd
from datetime import datetime

# ⚠️ UPDATE THESE VALUES
CLIENT_ID = "your_client_id_here"
CLIENT_SECRET = "your_client_secret_here"
ACCOUNT_NAME = "faircentindia"
DATABASE_NAME = "fair"
ENGINE_NAME = "general_purpose"

print("✅ Configuration set!")
```

---

### **Cell 3: Connect to Firebolt**

```python
print("🔌 Connecting to Firebolt...")

auth = ClientCredentials(CLIENT_ID, CLIENT_SECRET)
connection = connect(
    auth=auth,
    account_name=ACCOUNT_NAME,
    database=DATABASE_NAME,
    engine_name=ENGINE_NAME
)

print("✅ Connected successfully!")
```

---

### **Cell 4: Fetch All Column Data Types**

```python
print("📊 Fetching column data types...")

query = """
SELECT 
    table_name,
    column_name,
    data_type,
    is_nullable,
    ordinal_position
FROM information_schema.columns
WHERE table_schema = 'public'
ORDER BY table_name, ordinal_position
"""

cursor = connection.cursor()
cursor.execute(query)
results = cursor.fetchall()

df = pd.DataFrame(results, columns=[
    'table_name', 'column_name', 'data_type', 'is_nullable', 'ordinal_position'
])

print(f"✅ Found {len(df):,} columns across {df['table_name'].nunique()} tables")
```

---

### **Cell 5: View Summary**

```python
print("=" * 70)
print("  SUMMARY")
print("=" * 70)
print(f"Total Tables: {df['table_name'].nunique():,}")
print(f"Total Columns: {len(df):,}")
print(f"Unique Data Types: {df['data_type'].nunique()}")

print("\n📊 Top 10 Data Types:")
print(df['data_type'].value_counts().head(10))

# DECIMAL columns
decimal_count = len(df[df['data_type'].str.contains('decimal|numeric', case=False, na=False)])
print(f"\n🔢 DECIMAL/NUMERIC Columns: {decimal_count:,}")
```

---

### **Cell 6: View First 20 Rows**

```python
df.head(20)
```

---

### **Cell 7: Export to Excel**

```python
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
filename = f'firebolt_datatypes_{timestamp}.xlsx'

print(f"💾 Exporting to Excel: {filename}")

with pd.ExcelWriter(filename, engine='openpyxl') as writer:
    # All columns
    df.to_excel(writer, sheet_name='All Columns', index=False)
    
    # Table summary
    table_summary = df.groupby('table_name').agg({
        'column_name': 'count'
    }).rename(columns={'column_name': 'total_columns'}).reset_index()
    table_summary.to_excel(writer, sheet_name='Table Summary', index=False)
    
    # DECIMAL columns
    decimal_cols = df[df['data_type'].str.contains('decimal|numeric', case=False, na=False)]
    if len(decimal_cols) > 0:
        decimal_cols.to_excel(writer, sheet_name='DECIMAL Columns', index=False)
    
    # Data type distribution
    datatype_dist = df['data_type'].value_counts().reset_index()
    datatype_dist.columns = ['data_type', 'count']
    datatype_dist.to_excel(writer, sheet_name='Data Type Distribution', index=False)

print(f"✅ Excel file created: {filename}")
```

---

### **Cell 8: Close Connection**

```python
connection.close()
print("✅ Done! Check the Excel file in your current directory.")
```

---

## 🔍 Additional Useful Queries

### **View DECIMAL Columns Only:**

```python
decimal_cols = df[df['data_type'].str.contains('decimal|numeric', case=False, na=False)]
print(f"📊 Found {len(decimal_cols)} DECIMAL columns")
decimal_cols
```

---

### **Search Specific Table:**

```python
table_name = "users"  # Change to your table
table_cols = df[df['table_name'] == table_name]
print(f"📊 Columns in '{table_name}': {len(table_cols)}")
table_cols
```

---

### **Find Tables with Most Columns:**

```python
table_summary = df.groupby('table_name')['column_name'].count().reset_index()
table_summary.columns = ['table_name', 'column_count']
table_summary = table_summary.sort_values('column_count', ascending=False)
table_summary.head(20)
```

---

### **Search by Data Type:**

```python
search_type = "varchar"  # Change to: "int", "bigint", "date", etc.
matching = df[df['data_type'].str.contains(search_type, case=False, na=False)]
print(f"Found {len(matching)} columns with '{search_type}' type")
matching.head(20)
```

---

## 📊 Output Files

The Excel file will contain **4 sheets:**

1. **All Columns** - Complete list with all details
2. **Table Summary** - Column count per table
3. **DECIMAL Columns** - All DECIMAL/NUMERIC columns
4. **Data Type Distribution** - Count of each data type

---

## ⚠️ Troubleshooting

### **Error: "Cannot connect"**
- Check CLIENT_ID and CLIENT_SECRET
- Verify ACCOUNT_NAME, DATABASE_NAME, ENGINE_NAME
- Ensure engine is running in Firebolt

### **Error: "firebolt-sdk not found"**
- Run Cell 1 again to install packages
- Restart Jupyter kernel

### **Error: "No module named 'openpyxl'"**
- Run: `!pip install openpyxl`

---

## ✅ That's It!

You'll get:
- ✅ Complete list of all columns and data types
- ✅ Excel file with 4 analysis sheets
- ✅ Easy to filter and search

**Questions?** Contact your Firebolt team.

