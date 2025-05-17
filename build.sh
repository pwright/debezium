#!/usr/bin/env bash
set -euo pipefail

# Define constants
PLAYBOOK_FILE="antora-playbook.yml"
OUTPUT_DIR="./build/site"
UI_BUNDLE_URL="https://gitlab.com/antora/antora-ui-default/-/jobs/artifacts/main/raw/build/ui-bundle.zip?job=build"

# Extract component name from antora.yml
if [[ ! -f antora.yml ]]; then
  echo "Error: antora.yml not found in current directory." >&2
  exit 1
fi

COMPONENT_NAME=$(awk '/^name:/ { print $2; exit }' antora.yml)

if [[ -z "$COMPONENT_NAME" ]]; then
  echo "Error: Could not extract component name from antora.yml." >&2
  exit 1
fi

# Generate Antora playbook
cat > "$PLAYBOOK_FILE" <<EOF
site:
  title: Local Docs
  start_page: ${COMPONENT_NAME}::index.adoc

content:
  sources:
    - url: .
      branches: HEAD
      start_path: .

output:
  dir: ${OUTPUT_DIR}

ui:
  bundle:
    url: ${UI_BUNDLE_URL}
    snapshot: true
EOF

# Run Antora
echo "Running Antora using playbook: $PLAYBOOK_FILE"
npx antora "$PLAYBOOK_FILE"
