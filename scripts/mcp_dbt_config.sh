#!/usr/bin/env bash
# mcp_dbt_config.sh — check that dbt-mcp can run against this project, and
# print the client configuration to paste into an MCP client.
#
# dbt-mcp is dbt Labs' MCP server. It exposes dbt itself -- run, build, test,
# compile, model lineage, codegen -- over the Model Context Protocol, so an
# assistant can operate the transformation layer rather than only read its
# output tables.
#
# It is deliberately NOT vendored. A copy of it lived in this repository until
# #70 removed 469 files that nothing consumed: no image build, no manifest, no
# import. It is a published package; `uvx dbt-mcp` fetches it. This script
# checks the prerequisites and prints the wiring, which is the part that is
# specific to this repo.
#
# There is no "start the server" step on purpose. A stdio MCP server is
# launched by its client, on demand -- running one from a shell just leaves a
# process waiting on stdin.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
GREEN='\033[0;32m'; RED='\033[0;31m'; YELLOW='\033[1;33m'; NC='\033[0m'
ok()   { echo -e "${GREEN}[OK]${NC}    $*"; }
warn() { echo -e "${YELLOW}[WARN]${NC}  $*"; }
die()  { echo -e "${RED}[ERROR]${NC} $*"; exit 1; }

# ─── uv / uvx ────────────────────────────────────────────────────────────────
command -v uvx >/dev/null \
  || die "uvx not found. Install uv: https://docs.astral.sh/uv/getting-started/installation/"
ok "uvx found: $(command -v uvx)"

# ─── a dbt that actually runs ────────────────────────────────────────────────
# Checked by running it, not by existence. A dbt on PATH can be an editable
# install from a source checkout with a missing dependency, in which case the
# binary is present and every dbt-mcp tool fails at call time with a Python
# traceback that looks like an MCP problem.
DBT_BIN="${DBT_PATH:-$(command -v dbt || true)}"
[ -n "$DBT_BIN" ] || die "No dbt on PATH. Install one:
    python -m venv .venv-dbt
    .venv-dbt/bin/pip install 'dbt-core==1.9.*' 'dbt-postgres==1.9.*'
    DBT_PATH=\$PWD/.venv-dbt/bin/dbt make mcp-dbt
  Pin the version: an unpinned 'pip install dbt-core' can resolve to a 2.0
  pre-release that ships no 'dbt' console script at all."

if ! DBT_VERSION="$("$DBT_BIN" --version 2>&1)"; then
  die "dbt is present but does not run: $DBT_BIN
$(echo "$DBT_VERSION" | tail -3)
  If that traceback names a source checkout, this is an editable install with a
  missing dependency. Use a clean virtualenv as above and pass DBT_PATH."
fi
ok "dbt runs: $DBT_BIN ($(echo "$DBT_VERSION" | grep -m1 -o 'installed: [0-9.]*' || echo 'version unknown'))"

# ─── the dbt project ─────────────────────────────────────────────────────────
[ -f "$REPO_ROOT/dbt/dbt_project.yml" ] || die "No dbt project at $REPO_ROOT/dbt"
[ -f "$REPO_ROOT/dbt/profiles.yml" ]    || die "No profiles.yml at $REPO_ROOT/dbt"
ok "dbt project and profile found in $REPO_ROOT/dbt"

# ─── the warehouse the profile points at ─────────────────────────────────────
# profiles.yml reads DEST_DB_* with docker-compose defaults, so the values
# below are what the tools will actually connect with.
DEST_HOST="${DEST_DB_HOST:-localhost}"
DEST_PORT="${DEST_DB_PORT:-5433}"
if command -v nc >/dev/null && ! nc -z "$DEST_HOST" "$DEST_PORT" 2>/dev/null; then
  warn "Nothing listening on $DEST_HOST:$DEST_PORT — 'make up' first, or on"
  warn "Kubernetes port-forward postgres-dest. Tools that compile will work;"
  warn "'show' and 'test' need the warehouse."
else
  ok "warehouse reachable at $DEST_HOST:$DEST_PORT"
fi

# ─── the configuration ───────────────────────────────────────────────────────
# The four DISABLE_* flags below are not optional tidying. Those tool groups
# require a dbt Cloud account (DBT_HOST, DBT_TOKEN, DBT_PROD_ENV_ID and more);
# left enabled against dbt Core, dbt-mcp still advertises roughly 30 tools that
# fail the moment they are called. Disabling them takes the surface from 49
# tools to 16 that all work.
#
# DISABLE_DBT_CODEGEN is inverted: it defaults to true, so codegen has to be
# switched on rather than off.
cat <<EOF

────────────────────────────────────────────────────────────────────────────
Claude Code (one line, paste as-is):

  claude mcp add dbt -- env DBT_PROJECT_DIR=$REPO_ROOT/dbt DBT_PROFILES_DIR=$REPO_ROOT/dbt DBT_PATH=$DBT_BIN DEST_DB_HOST=$DEST_HOST DEST_DB_PORT=$DEST_PORT DISABLE_SEMANTIC_LAYER=true DISABLE_DISCOVERY=true DISABLE_ADMIN_API=true DISABLE_SQL=true DISABLE_DBT_CODEGEN=false uvx dbt-mcp

Claude Desktop, or any client taking JSON:

{
  "mcpServers": {
    "dbt": {
      "command": "uvx",
      "args": ["dbt-mcp"],
      "env": {
        "DBT_PROJECT_DIR": "$REPO_ROOT/dbt",
        "DBT_PROFILES_DIR": "$REPO_ROOT/dbt",
        "DBT_PATH": "$DBT_BIN",
        "DEST_DB_HOST": "$DEST_HOST",
        "DEST_DB_PORT": "$DEST_PORT",
        "DISABLE_SEMANTIC_LAYER": "true",
        "DISABLE_DISCOVERY": "true",
        "DISABLE_ADMIN_API": "true",
        "DISABLE_SQL": "true",
        "DISABLE_DBT_CODEGEN": "false"
      }
    }
  }
}
────────────────────────────────────────────────────────────────────────────

16 tools: build clone compile docs generate_model_yaml generate_source
generate_staging_model get_lineage_dev get_node_details_dev
get_product_doc_pages list parse run search_product_docs show test

'show' runs SQL through 'dbt show --inline' against the dbt target, so it
reaches the warehouse only. The Iceberg lakehouse and the ClickHouse mirror
are not visible to dbt and so not to dbt-mcp.
EOF
