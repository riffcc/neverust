#!/bin/bash
#
# Test script for manifest upload/download integration
#

set -e

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
NEVERUST_API="http://10.7.1.37:8080"
BLACKBERRY_API="http://10.7.1.200:8080"

echo -e "${BLUE}=== Manifest Integration Test ===${NC}\n"

# Test 1: Upload to Neverust
echo -e "${BLUE}Step 1: Uploading test data to Neverust...${NC}"
TEST_DATA="Hello from Neverust manifest integration test!"
CID=$(curl -s -X POST "$NEVERUST_API/api/archivist/v1/data" --data-binary "$TEST_DATA")

if [ -z "$CID" ]; then
    echo "ERROR: Upload failed!"
    exit 1
fi

echo -e "${GREEN}✓ Upload successful!${NC}"
echo "Manifest CID: $CID"
echo

# Test 2: Download from Neverust (verify upload worked)
echo -e "${BLUE}Step 2: Downloading from Neverust (verifying upload)...${NC}"
DOWNLOADED_NEVERUST=$(curl -s "$NEVERUST_API/api/archivist/v1/data/$CID/network/stream")

if [ "$DOWNLOADED_NEVERUST" = "$TEST_DATA" ]; then
    echo -e "${GREEN}✓ Download from Neverust successful!${NC}"
    echo "Data matches: $DOWNLOADED_NEVERUST"
else
    echo "ERROR: Data mismatch!"
    echo "Expected: $TEST_DATA"
    echo "Got: $DOWNLOADED_NEVERUST"
    exit 1
fi
echo

# Test 3: Try to download from Blackberry
echo -e "${BLUE}Step 3: Attempting to download from Blackberry...${NC}"
echo "(This will likely fail because Blackberry doesn't have the blocks yet)"
echo "CID to fetch: $CID"
echo

DOWNLOADED_BLACKBERRY=$(curl -s "$BLACKBERRY_API/api/archivist/v1/data/$CID/network/stream" || echo "FAILED")

if [ "$DOWNLOADED_BLACKBERRY" = "$TEST_DATA" ]; then
    echo -e "${GREEN}✓ SUCCESS! Downloaded from Blackberry!${NC}"
    echo "Data: $DOWNLOADED_BLACKBERRY"
elif [ "$DOWNLOADED_BLACKBERRY" = "FAILED" ]; then
    echo -e "${BLUE}Note: Blackberry doesn't have the blocks (expected)${NC}"
    echo "This is normal - blocks need to be propagated via P2P BlockExc"
else
    echo "Unexpected response from Blackberry: $DOWNLOADED_BLACKBERRY"
fi
echo

echo -e "${GREEN}=== Integration Test Complete ===${NC}"
echo
echo "Summary:"
echo "  - Neverust upload: ✓"
echo "  - Neverust download: ✓"
echo "  - Manifest CID: $CID"
echo
echo "Next steps:"
echo "  1. Implement BlockExc block propagation"
echo "  2. Or use HTTP fallback in download handler"
echo "  3. Then test cross-node download"
