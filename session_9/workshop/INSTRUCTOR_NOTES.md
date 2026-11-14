# Session 9 Workshop: instructor notes

## What this is

A hands-on workshop, no slides. Students work from their laptop (Git, VS Code, Databricks CLI):

1. look at the raw **USGS earthquake** data,
2. build a pipeline (3 SQL files) and a job (3 tasks) as **bundle files**,
3. deploy to **dev** and run,
4. push to the repo (Azure DevOps, GitHub works the same),
5. deploy to **prod** from a fresh clone of the repo,
6. ship one change: branch, dev, pull request, merge, deploy prod.

It does **not** use Databricks Git folders. See "Why no Git folder" below.

| File | Who reads it |
|---|---|
| `WORKSHOP_GUIDE.md` | students: every step, every file inline (generated from `quakes_repo_final/`) |
| `quakes_repo_final/` | you: the finished, working repo. Do not hand it out before the workshop |
| `INSTRUCTOR_NOTES.md` | you |

If you change a file in `quakes_repo_final/`, copy the change into the matching block of the guide. They are identical today (checked file by file).

## What changed after the first version, and why

The first version built everything by clicking in the workspace and used a Databricks Git folder to move code to prod. That failed in your workspace: no **Git folder** option, and trouble copying the Azure DevOps address. I checked the Databricks documentation and found:

- **A Git folder is not needed for bundles.** A bundle deploys from any folder where the CLI runs. A fresh `git clone` plus `databricks bundle deploy -t prod` is "deploy from the repo", and it is also what a CI agent does.
- **A real bug in my prod target.** I had `mode: production` with an explicit `root_path` containing the user name. The docs say production mode rejects a `root_path` set to a specific user (unless the job runs as a service principal) and wants `run_as` and `permissions`. That would have failed `bundle validate`. The prod target now has no `mode`, with a comment explaining what a real prod target adds.
- **Pipeline source** now uses `glob: include: ../src/pipeline/**`, which the docs call the unified field. The old `file:` entries are described as pointing at Repos files.
- Bundles are now called **Declarative Automation Bundles** in the docs. The commands are unchanged.
- The setup notebook is gone. It needed a Git folder. Students run four SQL statements in the SQL editor instead.
- Only two schemas now (`_dev`, `_prod`). The earlier four existed to keep the Git-folder path and the bundle path apart.

## Your Git folder problem, answered from the docs

- To create one: **Workspace** > browse to a folder > **Create** > **Git folder**. You need **CAN MANAGE** on the parent folder. If you only see **Folder**, you are probably somewhere you do not have that permission. Try your own folder under **Users**. Git folders are on by default, but an admin can switch the feature off.
- Azure DevOps address: `https://dev.azure.com/<org>/<project>/_git/<repo>`. The **Clone** button in Azure DevOps often shows it as `https://<org>@dev.azure.com/...`. Remove the `<org>@` part. The docs warn against addresses that contain user names.
- Credentials: if Azure DevOps and Databricks are in the same Microsoft Entra ID tenant, sign-in can be automatic. Otherwise add a personal access token in **Settings** > **Linked accounts** > **Add Git credential**.

The `_git` address shape and the `<org>@` detail come from how Azure DevOps works, not from a Databricks page. The docs pages do not state the address format.

Appendix A of the guide covers this for students who want a Git folder anyway.

## What was verified, and what was not

**Verified here, with real data (unchanged from before):**
- The download notebook against the live USGS API: 30 days = 30 files = 9,894 events. A second run skipped 28 days and re-fetched 2.
- The three SQL files and `99_verify.py` in local Spark: Bronze 10,396 rows, Silver 9,894, 139 regions, all checks pass.
- The `curl ... limit=3` command in Part 1 returns the header and 3 rows.
- The Part 5 edit gives the new column.
- The guide's code blocks equal the repo files; the dev-only and prod parts of `databricks.yml` together equal the repo file. YAML references all resolve.

**Checked against the Databricks docs, not run:**
- Notebook tasks without a cluster run on serverless; Python script tasks would need `environment_key`.
- `pipeline_task` with `${resources.pipelines.<key>.id}`; job `parameters` with `name` and `default`; the `schedule` block.
- `serverless: true` with `catalog`, `schema` and `configuration`.
- `bundle destroy` deletes deployed jobs, pipelines and artifacts, and asks first.
- `workspace.host` can be left out: the CLI uses the login profile.

**Not verified at all:**
- Nothing ran in a Databricks workspace, and the CLI was never run (the sandbox refused a download). `bundle validate` has not been run on these files.
- That a `.py` file starting `# Databricks notebook source` is deployed as a notebook by `bundle deploy`. The docs pages I read did not say. It is how Databricks source files work, and the earlier Session 9 demo relied on it.
- The pipeline-only SQL syntax (`CREATE OR REFRESH STREAMING TABLE`, `STREAM read_files`, `${quakes.landing_path}`, `CONSTRAINT ... DROP ROW`) in a real pipeline.
- Whether `${...}` pipeline configuration is allowed inside the `read_files` path in your workspace version.
- The drift demo (a hand edit in prod is reverted by the next deploy).
- Azure DevOps screen names (**Repos**, **Clone**, **Generate Git Credentials**, **Pull requests**).

## Do this before class (30 min, in the real workspace)

Do the whole workshop once yourself, alone, from Part 0. Most likely to bite first:

1. **Outbound internet from serverless compute.** If the download task fails with a network error, ask the admin to allow `earthquake.usgs.gov`. Quick test in a notebook:
   ```python
   import urllib.request
   print(urllib.request.urlopen("https://earthquake.usgs.gov/fdsnws/event/1/count?starttime=2026-11-01&endtime=2026-11-02", timeout=30).read())
   ```
2. **`databricks bundle validate -t dev` and `-t prod`.** Watch for: the `glob` entry under `libraries`, `serverless: true`, and whether the `.py` files arrive as notebooks.
3. **CLI version.** The pipeline tutorial in the docs asks for 0.283.0 or newer for `databricks pipelines init`. Our files are hand-written, so a recent CLI should do, but installing the latest is the safe answer.
4. **Schema permission.** Each student must be able to create a schema and a volume in the catalog.
5. **Repo access.** Each student needs to create a repo and push to it. Check branch policies on `main`: if `main` is protected, the first push in Part 3 fails and they need the branch and pull request route.
6. **Homebrew.** The docs say newer Homebrew needs `brew trust databricks/tap` before `brew install databricks`.

Fix whatever breaks in `quakes_repo_final/` and the guide.

## Timing

| Part | Min | Notes |
|---|---|---|
| 0 Prepare | 20 | CLI install and login are the slow part. Ask students to do 0.1 beforehand |
| 1 Raw data | 10 | the answer to "what are we downloading" |
| 2 Build and deploy dev | 60 | the longest. The first job run takes several minutes |
| 3 Push | 15 | |
| 4 Deploy prod | 30 | |
| 5 Ship a change | 30 | cut the optional drift demo if short |
| 6 Clean up | 10 | |

About 2 h 55. 

## What to say at each "See"

| See | Say |
|---|---|
| 1 | The source is a web address that returns CSV. Nothing to log in to. We save each reply untouched as a file. |
| 2 | A pipeline is declarative. You describe tables, the order comes from who reads whom. |
| 3 | Three actions for a bad row: warn (keep, count), drop, fail the pipeline. We used warn and drop. |
| 4 | Verify turns a silent wrong result into a red job run. |
| 5 | Bronze keeps every version; Silver keeps one. That is why Bronze grows and Silver barely moves. |
| 6 | The key point of Part 3: the repo holds the definition of the pipeline and the job, not just code. |
| 7 | One set of files, two environments. Only the target differs. |
| 8 | Prod changed only because `main` changed. `quakes-deploy` is never edited. |
| 9 | The repo is the truth. A hand edit in prod does not survive the next deploy. |

## Design choices, if students ask

- **Laptop and CLI, not the workspace UI.** It works in any workspace, needs no Git folder, and is how a CI pipeline will do it later.
- **SQL pipeline, not Python.** Shorter, closer to what data engineers read, and no dependence on the pipeline Python module name, which has changed between releases.
- **Schemas, not workspaces, as environments.** Every student can do it in the one workspace they have. For a second workspace, add `workspace: host:` under the prod target (comment in `databricks.yml`).
- **No `mode: production` on prod.** It needs a service principal. The guide's last section says what a real prod target adds.
- **Literal `<placeholders>` in `databricks.yml`.** The docs do not say that a variable can be built from another variable, so each target holds plain values.
- **A second clone as the "build server".** It shows that prod is deployed from what is in `main`, with no local edits, without needing CI.
- **No CI.** Out of scope on purpose. It is the natural next session: a check on every pull request, then deploy on merge.
- **Gold counts earthquakes only.** Silver keeps quarry blasts, explosions, ice quakes. Silver preserves, Gold decides.

## Real numbers to expect (they move every day)

About 300 events per day. 30 days: roughly 10,000 rows, 30 files. After a second run: 32 files, Bronze about 500 rows larger than Silver. About 140 regions. About 16% of events are still `automatic` (not reviewed). About 3% of events are not earthquakes.

## Sources used for the corrections

- Create and manage Git folders: <https://learn.microsoft.com/en-us/azure/databricks/repos/git-operations-with-repos>
- Configure Git integration: <https://learn.microsoft.com/en-us/azure/databricks/repos/repos-setup>
- Bundle deployment modes: <https://learn.microsoft.com/en-us/azure/databricks/dev-tools/bundles/deployment-modes>
- Bundle resources (pipeline, job): <https://learn.microsoft.com/en-us/azure/databricks/dev-tools/bundles/resources>
- Bundle examples (serverless, pipeline task, schedule): <https://learn.microsoft.com/en-us/azure/databricks/dev-tools/bundles/examples>
- Pipelines with bundles tutorial: <https://learn.microsoft.com/en-us/azure/databricks/dev-tools/bundles/pipelines-tutorial>
- CLI install: <https://learn.microsoft.com/en-us/azure/databricks/dev-tools/cli/install>

## Note on the other Session 9 folder

`session_9_deploy_demo/` (taxi data, Azure Repos, CI/CD with Azure Pipelines) is a different design and is untouched. This workshop does not depend on it.
