# MX Domain Workflow - Quick Start Guide

## Overview

You now have an optimized workflow for MX scanning with 38M emails already loaded in the database.

## The Workflow (5 Steps)

### Step 1: Extract Domains (One-time backfill)
Extract all unique domains from the emails table into `domain_mx`:

```bash
cd "c:\Users\mdbar\OneDrive\Documents\Email Data 2022\email_processor"
python mx_domain_ops.py --backfill
```

**What it does:** Builds the domain list so MX scan doesn't need to query the big emails table.
**Time:** 5-15 minutes

---

### Step 2: MX Scan (Operation 1 - DNS intensive)
Run the MX validator to scan each domain:

```bash
python mx_validator.py --workers 16
```

**What it does:** 
- DNS lookup for each domain
- Classifies as deliverable/undeliverable
- Categorizes as GI/Cable/Big4
- Identifies provider (Google, Yahoo, Spectrum, etc.)
- Saves results to `domain_mx` table

**Time:** Hours (depends on domain count, network speed, workers)
**Note:** This is the long-running DNS operation. Run overnight or in background.

---

### Step 3: Move Undeliverables (After MX scan)
Move undeliverable emails to a separate backup table:

```bash
python mx_domain_ops.py --move-undeliverable
```

**What it does:** Creates `emails_undeliverable` table, moves emails with undeliverable domains there, deletes from main table.
**Time:** 10-30 minutes

---

### Step 4: CLUSTER (Optimize for updates)
Physically sort the emails table by domain:

```bash
python mx_domain_ops.py --cluster
```

**What it does:** Reorders the entire `emails` table so all emails for the same domain are next to each other on disk.
**Time:** 10-30 minutes
**Note:** Makes Step 5 much faster.

---

### Step 5: Apply MX Data (Chunked updates)
Copy MX data from `domain_mx` to `emails` table in chunks:

```bash
python mx_domain_ops.py --apply
```

**What it does:** Updates each email row with `mx_category`, `mx_valid`, `mx_host_provider` from the domain's row in `domain_mx`.
**Time:** 30-60 minutes
**Note:** Fast because table is clustered.

---

## Run All Post-MX Steps at Once

After MX scan is complete, you can run steps 3, 4, and 5 together:

```bash
python mx_domain_ops.py --all
```

---

## Full Workflow Example

```bash
# Starting from: 38M emails already loaded in database

# Step 1: Extract domains (one-time)
python mx_domain_ops.py --backfill

# Step 2: MX scan (long-running, can take hours)
python mx_validator.py --workers 16

# After MX scan completes, run all post-processing steps:
python mx_domain_ops.py --all
```

---

## What Gets Updated

After the full workflow, each email row will have:

- **mx_valid**: `true` (deliverable) or `false` (undeliverable)
- **mx_category**: `GI` / `Cable` / `Big4` / `Real_GI` / etc.
- **mx_host_provider**: Provider name (e.g., `Google`, `Yahoo`, `Spectrum`)

Plus:
- **emails_undeliverable** table: Contains all emails with undeliverable domains (archived, not deleted)
- **emails** table: Contains only deliverable emails, physically sorted by domain for fast queries

---

## Resume / Re-run

- **MX scan** can be resumed (it tracks checked domains in `domain_mx`). Run `python mx_validator.py` again and it will pick up where it left off.
- **Other steps** can be re-run safely (they use `ON CONFLICT` or check for existing tables).

---

## Files Changed

1. **mx_domain_ops.py** (NEW): Handles steps 1, 3, 4, 5
2. **mx_validator.py** (UPDATED): `get_unchecked_domains()` now reads from `domain_mx` only (no emails scan)

---

## Need Help?

- Check table: `SELECT COUNT(*) FROM domain_mx;` (should have millions of domains after step 1)
- Check MX progress: `SELECT COUNT(*) FROM domain_mx WHERE mx_primary IS NOT NULL;` (increases during step 2)
- Check deliverables: `SELECT COUNT(*) FROM domain_mx WHERE is_valid = true;`
- Check undeliverables: `SELECT COUNT(*) FROM emails_undeliverable;` (after step 3)
