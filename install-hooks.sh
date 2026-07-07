#!/bin/bash
# M936: Install pre-commit secret scanner
# Run: bash install-hooks.sh (from repo root)
HOOK=".git/hooks/pre-commit"
cat > "$HOOK" << 'HOOK_CONTENT'
#!/bin/bash
RED='\033[0;31m'; YELLOW='\033[1;33m'; NC='\033[0m'
PATTERNS=(
  'ghp_[a-zA-Z0-9]{36}'
  'hf_[a-zA-Z0-9]{20,}'
  'sk-[a-zA-Z0-9]{20,}'
  'sb_[a-zA-Z0-9]{20,}'
  'supabase.*service_role'
  'pypi-[a-zA-Z0-9\-_]{40,}'
  'r8_[a-zA-Z0-9]{40}'
  'sk-ant-[a-zA-Z0-9\-_]{40,}'
  'AIza[0-9A-Za-z\-_]{35}'
  'ya29\.[0-9A-Za-z\-_]+'
)
FOUND=0
for FILE in $(git diff --cached --name-only --diff-filter=ACM); do
  git show ":$FILE" | file - | grep -q binary && continue
  for P in "${PATTERNS[@]}"; do
    M=$(git show ":$FILE" | grep -iE "$P" | head -1)
    if [ -n "$M" ]; then
      echo -e "${RED}SECRET DETECTED${NC} in ${YELLOW}$FILE${NC}: $P"
      FOUND=1
    fi
  done
done
[ $FOUND -eq 1 ] && echo -e "\n${RED}Commit BLOCKED.${NC}" && exit 1
exit 0
HOOK_CONTENT
chmod +x "$HOOK"
echo "Pre-commit hook installed at $HOOK"
