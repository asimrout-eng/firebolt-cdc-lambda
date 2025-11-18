# ✅ Error Handling Best Practices (HTTP Status Codes)

## 🎯 Overview

The Lambda function now uses **HTTP status codes** instead of text-based error matching for retry logic, following Firebolt's official best practices for explicit transactions.

---

## 📊 HTTP Status Code Handling

| Status Code | Category | Meaning | Action |
|-------------|----------|---------|--------|
| **409** | Conflict | Transaction conflict (MVCC) | ✅ **RETRY** with exponential backoff |
| **5xx** | Server Error | Transient server problem | ✅ **RETRY** with exponential backoff |
| **4xx** | Client Error | Permanent error (bad request) | ❌ **DON'T RETRY** - Fix underlying issue |
| **2xx** | Success | Operation completed successfully | ✅ **PROCEED** |

---

## 🔧 Implementation Details

### **Status Code Extraction**

The Lambda code checks for HTTP status codes in two ways:

```python
# Method 1: Direct attribute
if hasattr(e, 'status_code'):
    status_code = e.status_code

# Method 2: Via response object
elif hasattr(e, 'response') and hasattr(e.response, 'status_code'):
    status_code = e.response.status_code
```

### **Retry Decision Logic**

```python
if status_code == 409:
    # Transaction conflict
    is_retryable = True
    error_category = "Conflict (409)"
    
elif 500 <= status_code < 600:
    # Server error (timeout, unavailability)
    is_retryable = True
    error_category = f"Server Error ({status_code})"
    
elif 400 <= status_code < 500:
    # Client error (syntax, constraint violation)
    is_retryable = False
    error_category = f"Client Error ({status_code})"
```

### **Fallback to Text Matching**

If no HTTP status code is available (older SDK, network errors), the code falls back to text-based matching:

```python
if ("conflict" in error_msg.lower() or 
    "detected 1 conflicts" in error_msg or
    "cannot be retried" in error_msg.lower()):
    is_retryable = True
    error_category = "Conflict (text match)"
```

---

## 📝 Example Error Logs

### **✅ Retryable Error (409 Conflict):**

```
⚠️  Conflict (409) on users, retry 1/3 in 2.34s: Transaction conflict detected
⚠️  Conflict (409) on users, retry 2/3 in 5.12s: Transaction conflict detected
✓ MERGE completed for users (1234 rows affected)
```

### **✅ Retryable Error (503 Server Error):**

```
⚠️  Server Error (503) on sessions, retry 1/3 in 2.78s: Service temporarily unavailable
✓ MERGE completed for sessions (567 rows affected)
```

### **❌ Non-Retryable Error (400 Client Error):**

```
✗ MERGE failed for orders with non-retryable error (Client Error (400)): 
  Syntax error near 'SELCT'
   Columns used: ['id', 'customer_id', 'total']
   Primary keys: ['id']
```

### **❌ Non-Retryable Error (DECIMAL mismatch):**

```
✗ MERGE failed for cent_borrower_term_condition with non-retryable error (Client Error (400)):
  numeric(38, 0) can't be assigned to column loan_id of the type numeric(20, 0)
   Columns used: ['id', 'uid', 'created']
   Primary keys: ['id']
```

---

## 🔄 Retry Strategy

**Exponential Backoff with Jitter:**

```python
wait_time = (2 ** attempt) + random.uniform(0, 1)
```

**Retry Sequence:**
- Attempt 1: Immediate
- Attempt 2: Wait ~2-3 seconds
- Attempt 3: Wait ~4-5 seconds
- Attempt 4: Fail (max 3 retries)

**Why Jitter?**
- Prevents "thundering herd" problem
- Spreads retry attempts across time
- Reduces load spikes on Firebolt engine

---

## 🚨 Special Cases

### **Case 1: Transaction Already Closed (Obsolete)**

**Note:** This no longer applies since we use `autocommit=True` (no explicit transactions).

**Legacy behavior (if using explicit transactions):**
```
Error: "Cannot ROLLBACK transaction: no transaction is in progress"
Action: Do nothing, transaction already clean
```

### **Case 2: DECIMAL Precision Mismatch**

**Error:** `numeric(38, 0) can't be assigned to column of type numeric(20, 0)`

**Status Code:** 400 (Client Error)

**Action:** 
- ❌ Don't retry (non-retryable)
- ✅ Lambda logs warning and skips DECIMAL columns
- ✅ Run schema fix SQL to recreate table

**See:** `HOW_TO_FIX_DECIMAL_PRECISION_ERRORS.md`

---

## 📊 Monitoring & Debugging

### **Check Retry Patterns:**

```bash
aws logs tail /aws/lambda/firebolt-cdc-processor \
  --since 1h \
  --region ap-south-1 \
  | grep "retry"
```

### **Check Non-Retryable Errors:**

```bash
aws logs tail /aws/lambda/firebolt-cdc-processor \
  --since 1h \
  --region ap-south-1 \
  | grep "non-retryable"
```

### **Check Status Codes:**

```bash
aws logs tail /aws/lambda/firebolt-cdc-processor \
  --since 1h \
  --region ap-south-1 \
  | grep -E "(409|5[0-9]{2}|4[0-9]{2})"
```

---

## 🎯 Benefits of Status Code Approach

| Benefit | Description |
|---------|-------------|
| **Deterministic** | No ambiguity - 409 always means conflict |
| **Language-agnostic** | Works across error messages in any language |
| **Future-proof** | Firebolt can change error messages without breaking logic |
| **Standard** | HTTP status codes are universal web standard |
| **Debuggable** | Clear categorization in logs |

---

## 📚 References

**Firebolt Official Documentation:**
- Explicit Transactions Error Handling Best Practices
- HTTP Status Code Meanings

**AWS Lambda Best Practices:**
- Error Handling and Retries
- Exponential Backoff

**Code Files:**
- `lambda/handler.py` - `execute_merge_with_retry()` function (lines 218-330)

---

## ✅ Summary

**Old Approach (Text Matching):**
```python
if "conflict" in error_msg.lower():
    retry()
```
❌ Fragile, language-dependent, ambiguous

**New Approach (HTTP Status Codes):**
```python
if status_code == 409:
    retry()
```
✅ Robust, deterministic, standard

---

**This is a production-grade implementation following Firebolt's official best practices.** 🎯

