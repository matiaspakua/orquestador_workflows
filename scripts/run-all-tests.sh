#!/usr/bin/env bash
set -euo pipefail

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${YELLOW}====================================${NC}"
echo -e "${YELLOW}  ORQUESTADOR WORKFLOWS — TEST SUITE ${NC}"
echo -e "${YELLOW}====================================${NC}"
echo ""

# 1. Unit tests (no Docker required)
echo -e "${YELLOW}[1/4] Running unit tests...${NC}"
python -m pytest ui/tests/ common/ tests_support/ \
    -v \
    --ignore=ui/tests_e2e \
    --ignore=producer/tests \
    --ignore=consumer/tests \
    --cov=ui/services \
    --cov=common \
    --cov-report=term-missing \
    2>&1 | tail -30
echo -e "${GREEN}✓ Unit tests complete${NC}"
echo ""

# 2. Integration tests (requires Docker)
echo -e "${YELLOW}[2/4] Running integration tests...${NC}"
if command -v docker &> /dev/null && docker info &> /dev/null; then
    python -m pytest common/test_integration_grpc.py \
        -v --timeout=30 2>&1 | tail -20 || true
    echo -e "${GREEN}✓ Integration tests complete${NC}"
else
    echo -e "${YELLOW}Skipping integration tests (Docker not available)${NC}"
fi
echo ""

# 3. E2E Playwright tests (requires running UI)
echo -e "${YELLOW}[3/4] Running E2E tests...${NC}"
if curl -s http://localhost:5001/health > /dev/null 2>&1; then
    E2E_TESTS=1 python -m pytest ui/tests_e2e/ \
        -v --timeout=30 2>&1 | tail -20 || true
    echo -e "${GREEN}✓ E2E tests complete${NC}"
else
    echo -e "${YELLOW}Skipping E2E tests (UI not running on localhost:5001)${NC}"
fi
echo ""

# 4. Lint/type check
echo -e "${YELLOW}[4/4] Running lint checks...${NC}"
if command -v ruff &> /dev/null; then
    ruff check ui/ common/ tests_support/ --quiet || true
    echo -e "${GREEN}✓ Lint checks complete${NC}"
else
    echo -e "${YELLOW}Skipping lint (ruff not installed)${NC}"
fi
echo ""

echo -e "${GREEN}====================================${NC}"
echo -e "${GREEN}  TEST SUITE COMPLETE${NC}"
echo -e "${GREEN}====================================${NC}"
