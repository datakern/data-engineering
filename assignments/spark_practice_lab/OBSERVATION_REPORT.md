![DataKern Watermark](https://img.shields.io/badge/DATAKERN-Spark%20Practice%20Lab-F26522?style=for-the-badge)
_www.data-kern.com_

# Observation Report: Spark Practice Lab

> ## 🎯 The idea of this lab
> This lab is **not** a test of whether you can invent Spark code. Its goal is to build **developer memory**: how a working data engineer handles Spark. *Look at the data → write a transformation → run it → check the result → fix what is wrong → move on.* You repeat that loop until the core moves come from your fingers, not from a search box.
>
> You **retype** the code in each notebook and run it. After **every run** you write down what you observed, in the 📝 cell right under it. This report is your **dev log**: the roll-up a developer keeps of what they did, what broke and what they learned.

**Name:** ____________________  **Date:** ____________________
**My machine:** Spark version ________ · cores ________ · OS ________ *(printed by `get_spark()` in Lab 1)*

---

## How to fill this in

There are two places where you write, and they do different jobs:

| Where | What you write | When |
|---|---|---|
| **In the notebook**, under each run (📝 Observe) | What you saw, in your own words, while the output is in front of you | After every run |
| **In this report**, at the end of each lab | The **row-count ledger**, the **errors you hit**, and the **moves you can type from memory** | Once per lab, after Run All |

> **The evidence rule:** write numbers and exact error lines from **your own run**, not words like "a lot" or "it failed". If you cannot say *why* something happened, write what you think and mark it `(unsure)`. An honest `(unsure)` earns more than a confident guess that is wrong.

> **AI use rule:** You may use AI tools (ChatGPT, Gemini, Claude, GitHub Copilot, Cursor, or similar) to help you write code. What matters is that you **understand and can reason about every line you submit**. AI can be confidently wrong, so treat its output as a suggestion from a fast but unreliable colleague, not as the answer: read it, run it, test it, and question it. If you cannot explain why your solution works, it is not yet your solution.
> Use AI to *explain* what you saw **after** you have written your own observation. Never paste an AI explanation as your observation.

---

## Lab 1 · Read: get the data in correctly

### 1. Row-count ledger
A developer always reconciles: *what did I get* against *what I should have got*. Fill the last two columns **after** you have finished the lab, when you know the true numbers.

| # | What I read, and how | Rows I got | Rows I **should** have got | Why they differ (or don't) |
|---|---|---|---|---|
| 1 | `orders`, no options | | | |
| 2 | `orders`, `header=True` | | | |
| 3 | `reviews`, plain read | | | |
| 4 | `reviews`, `multiLine=True` | | | |
| 5 | `reviews`, plain read, `DROPMALFORMED`, columns touched | | | |
| 6 | `orders_typed`, `FAILFAST`, columns touched | | | |

### 2. Errors and surprises I hit
| What I typed or ran | The error or surprise (exact line) | What it meant | How I fixed or explained it |
|---|---|---|---|
| | | | |
| | | | |

### 3. Moves I can now type from memory
Tick only what you could type **without looking**. Be honest: this list is for you.

- [ ] `spark.read.csv(path, header=True)`
- [ ] `spark.read.csv(path, header=True, schema=DDL)`
- [ ] `multiLine=True, quote='"', escape='"'`
- [ ] `mode="FAILFAST"`
- [ ] `df.printSchema()` · `df.show(5)` · `df.count()`
- [ ] `df.filter(F.col("x") == value)`
- [ ] `df.agg(F.count("column")).first()[0]`
- [ ] `left.join(right, "key")`

### 4. One thing I would tell a new teammate about reading CSVs in Spark
&nbsp;

---

<!-- Labs 2 to 6 are added here as each notebook is released. -->

## Final reflection *(after the last lab)*

Answer with specifics from **your own runs**, not general statements.

1. Which of your guesses was the most wrong, and what did the evidence teach you?
2. Pick one number from your ledgers and explain what Spark did underneath to produce it.
3. If a teammate said "the job didn't fail, so the data must be fine", what would you show them from this lab?
4. Which moves are still not in your fingers yet, and what will you do to practise them?

&nbsp;

---

*Don't focus on getting the answer. Focus on learning how to engineer the solution.*
**— Uma Kiran | DataKern | www.data-kern.com**
