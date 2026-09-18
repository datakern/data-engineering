# Session 2 — In-Class Exercises
### OOP & Pandas · DataKern Data Engineering

---

## What's inside

```
session_2/
├── exercises/          ← your working files (edit these)
│   ├── exercise_01_product_class.py
│   ├── exercise_02_customer_hierarchy.py
│   ├── exercise_03_order_item.py
│   ├── exercise_04_pandas_load_select.py
│   ├── exercise_05_pandas_filter_group.py
│   └── exercise_06_pandas_clean_export.py
└── solutions/          ← check AFTER you've tried it yourself
```

---

## Step 1 — Clone the DataKern repo (first time only)

```bash
git clone https://github.com/mindednet/datakern.git
cd datakern
```

> If you already cloned it, just pull the latest changes:
> ```bash
> git checkout main
> git pull origin main
> ```

---

## Step 2 — Create your branch

Name your branch using your first name and the session number:

```bash
git checkout -b session-2/firstname-lastname
```

**Example:**
```bash
git checkout -b session-2/alice-johnson
```

> ⚠️ Never work directly on `main`. Always use your own branch.

---

## Step 3 — Create a virtual environment

```bash
# Create the venv (do this once)
python3 -m venv .venv
```

**Activate it:**

| OS | Command |
|----|---------|
| macOS / Linux | `source .venv/bin/activate` |
| Windows | `.venv\Scripts\activate` |

You should see `(.venv)` appear in your terminal prompt.

---

## Step 4 — Install the required libraries

```bash
pip install pandas
```

Verify everything works:

```bash
python3 -c "import pandas; print('pandas', pandas.__version__)"
```

You should see something like: `pandas 3.0.6`

---

## Step 5 — Solve the exercises

Open the `exercises/` folder. Each file has clear comments telling you exactly what to build.

| Exercise | Topic | Run with |
|----------|-------|----------|
| `exercise_01_product_class.py` | Encapsulation — `Product` class | `python3 exercise_01_product_class.py` |
| `exercise_02_customer_hierarchy.py` | Inheritance & Polymorphism | `python3 exercise_02_customer_hierarchy.py` |
| `exercise_03_order_item.py` | Object Composition — `OrderItem` | `python3 exercise_03_order_item.py` |
| `exercise_04_pandas_load_select.py` | Load CSV, `.loc`, `.iloc`, Series | `python3 exercise_04_pandas_load_select.py` |
| `exercise_05_pandas_filter_group.py` | Boolean masking, `groupby` | `python3 exercise_05_pandas_filter_group.py` |
| `exercise_06_pandas_clean_export.py` | `fillna`, `dropna`, `to_csv` | `python3 exercise_06_pandas_clean_export.py` |

**Tip:** Run the file after every task to see your output immediately.

---

## Step 6 — Save your work to GitHub

After completing each exercise, commit your progress:

```bash
# See which files you've changed
git status

# Stage your exercise files
git add data_engineering_regular_sessions/session_2/exercises/

# Commit with a clear message
git commit -m "session-2: complete exercise 01 - Product class"

# Push your branch
git push origin session-2/firstname-lastname
```

Commit after each exercise — don't batch everything at the end.

---

## Step 7 — Raise a Pull Request

1. Go to your repository on **GitHub**
2. Click **"Compare & pull request"** (appears after you push)
3. Set the PR details:

| Field | Value |
|-------|-------|
| **Title** | `Session 2 Exercises — Firstname Lastname` |
| **Base branch** | `main` |
| **Compare branch** | `session-2/firstname-lastname` |

4. In the **description**, briefly answer:
   - What was the hardest exercise and why?
   - What does Polymorphism solve that IF statements can't handle at scale?

5. Click **"Create pull request"**

---

## Rules

- ❌ No AI tools (ChatGPT, Copilot, Claude, Cursor) for writing the code
- ✅ Use the Python docs, your notes, and ask the instructor
- ✅ Check the `solutions/` folder **only after** you've made a genuine attempt
- ✅ One branch per session — do not reuse old branches

---

## Quick reference

```bash
# Full workflow in one place
git clone https://github.com/mindednet/datakern.git   # first time only
cd datakern
git checkout main && git pull origin main
git checkout -b session-2/firstname-lastname
python3 -m venv .venv && source .venv/bin/activate
pip install pandas
# ... solve exercises inside data_engineering_regular_sessions/session_2/exercises/ ...
git add data_engineering_regular_sessions/session_2/exercises/
git commit -m "session-2: all exercises complete"
git push origin session-2/firstname-lastname
# then open GitHub → raise PR to main on mindednet/datakern
```
