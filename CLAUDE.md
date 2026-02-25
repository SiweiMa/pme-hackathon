# Project Rules — PWE Hackathon

## Project Context

This is an isolated hackathon project. It connects to an AWS account but must NEVER touch, modify, or interfere with any existing AWS resources or services.

## Isolation Rules (MANDATORY)

- **No SoFi/Galileo internal services**: Do not connect to any internal APIs, databases, Snowflake, or any production/staging/dev environment outside AWS.
- **No modifying files outside this repo**: Never write to, read from, or reference paths outside `/Users/sma/Documents/github/pwe-hackathon` (or the active worktree).
- **No installing global packages**: Use project-local dependencies only (`node_modules/`, `venv/`, etc.). Never `pip install --user`, `npm install -g`, or `brew install` unless explicitly asked.
- **No port conflicts**: If running a local server, use ports **9000–9099** to avoid colliding with any existing services.
- **No Docker interference**: Do not stop, restart, or modify any running Docker containers. If this project needs Docker, use a project-specific `docker-compose.yml` with a unique project name and isolated network.
- **No modifying system configs**: Do not touch `/etc/hosts`, shell profiles, SSH configs, or any system-level configuration.

## AWS Rules (CRITICAL)

AWS access is allowed, but existing resources are **off-limits**. Follow these rules strictly:

### CREATE only — never modify or delete existing resources
- **Only create NEW resources** with a `pwe-hackathon-` prefix in the name/tag so they are clearly identifiable.
- **Tag everything** you create with `Project=pwe-hackathon` so resources can be found and cleaned up easily.
- **Never modify, update, delete, or reconfigure** any pre-existing AWS resource (Lambda, S3 bucket, DynamoDB table, IAM role, VPC, security group, EC2 instance, RDS, ECS service, etc.).
- **Never change IAM policies, roles, or permissions** that already exist. Create new roles/policies only if needed, prefixed with `pwe-hackathon-`.

### Networking
- Do not modify existing VPCs, subnets, security groups, or route tables.
- If a VPC is needed, create a new one with `pwe-hackathon-` prefix.

### Storage
- Do not read from, write to, or delete objects in existing S3 buckets.
- Create new buckets with `pwe-hackathon-` prefix if needed.
- Do not touch existing DynamoDB tables, RDS instances, or any other data store.

### Compute
- Do not stop, restart, or modify existing EC2 instances, ECS services, or Lambda functions.
- New Lambdas/containers must use `pwe-hackathon-` prefix.

### Before any AWS CLI/SDK call — ask yourself:
1. Am I targeting a resource that already exists? If yes — **STOP, do not proceed**.
2. Does my new resource name start with `pwe-hackathon-`? If no — **rename it**.
3. Could this action affect an existing service (e.g., modifying a shared IAM role, changing a security group used by others)? If yes — **STOP, do not proceed**.

### Cleanup
- Keep a list of all AWS resources created in `AWS_RESOURCES.md` so they can be torn down after the hackathon.

## Data Handling

- The CSV (`Hackathon_customer_data.csv`) contains **synthetic/fake** customer data for hackathon use only.
- Never send this data to external APIs, LLMs, or third-party services.
- Never commit real credentials, tokens, or secrets. Use `.env` files (gitignored) for any config.

### Credentials

- All access credentials (AWS keys, API tokens, DB passwords, etc.) live in **`/tmp/credentials/`**.
- Read credentials from `/tmp/credentials/` at runtime — never copy them into the repo, `.env` files, or source code.
- Never log, print, or echo credential values to stdout/stderr.
- Never commit any file from `/tmp/credentials/` or inline their contents anywhere in the codebase.
- If a script or config needs a credential, reference the file path (e.g., `/tmp/credentials/aws_access_key`) rather than the value itself.
- **Credentials are IAM user access keys** (long-lived). They do not expire automatically, but must still be kept secret. On any `AccessDenied` or `InvalidClientTokenId` error, verify the key is correct and has the required permissions before retrying.

## Development Defaults

- **Language/framework**: Follow the user's lead. No assumptions.
- **Dependencies**: Always ask before adding a new dependency.
- **Virtual environments**: If Python is used, create a local `venv/` inside the project.

## Git Workflow (Worktree-Based)

This project uses the **git worktree workflow**. All work happens in worktrees — never in the base repo.

### Paths

| Purpose | Path |
|---------|------|
| Base repo (control plane, always clean) | `~/Documents/github/pwe-hackathon` |
| Worktrees | `~/Documents/github/pwe-hackathon-wt/` |

### Branch Naming

Use Conventional Commits prefixes:

```
feat/short-description
fix/short-description
chore/short-description
docs/short-description
```

### Task Lifecycle

```bash
# 1. Prepare base repo
cd ~/Documents/github/pwe-hackathon
git fetch origin
git checkout main && git pull --ff-only

# 2. Create worktree + branch
git worktree add -b feat/my-feature \
  ../pwe-hackathon-wt/feat-my-feature \
  origin/main
cd ../pwe-hackathon-wt/feat-my-feature

# 3. Work, commit atomically
git add -p
git commit -m "feat: add login endpoint"

# 4. Push + PR
git push -u origin feat/my-feature

# 5. Cleanup after merge
cd ~/Documents/github/pwe-hackathon
git worktree remove ../pwe-hackathon-wt/feat-my-feature
git branch -d feat/my-feature
```

### Hard Rules

| Rule | Enforced |
|------|----------|
| Never commit directly on `main` | YES |
| One task = one branch = one worktree | YES |
| Conventional Commits (`feat:`, `fix:`, `chore:`, `docs:`) | YES |
| Atomic commits (one logical change per commit) | YES |
| No mixing refactor + behavior + formatting in one commit | YES |
| Remove worktree after PR merge | YES |
| PR description derived from `git diff main...HEAD` | YES |

### Parallel Work

Multiple features can run simultaneously — each in its own worktree:

```bash
git worktree add -b feat/api ../pwe-hackathon-wt/feat-api origin/main
git worktree add -b feat/frontend ../pwe-hackathon-wt/feat-frontend origin/main
```

### .gitignore Essentials

These must be gitignored (create/update `.gitignore` if missing):

```
.env
.env.*
*.pem
*.key
venv/
node_modules/
__pycache__/
.DS_Store
AWS_RESOURCES.md
```

### Commit Message Format

```
<type>: <short summary in imperative mood>

[optional body — what and why, not how]
```

Examples:
- `feat: add customer lookup API`
- `fix: handle empty CSV rows gracefully`
- `chore: add .gitignore for env files`
- `docs: add API usage examples`

## What NOT to Do

- Do not run `kill`, `pkill`, or signal any process not started by this project.
- Do not run database migrations against any existing database.
- Do not modify, delete, or reconfigure any pre-existing AWS resource.
- Do not push to any remote other than `origin` (the hackathon repo).
- Do not create cron jobs, launchd agents, or background daemons.
