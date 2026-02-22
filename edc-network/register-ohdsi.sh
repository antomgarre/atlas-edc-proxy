#!/bin/bash

echo "1. Creating OHDSI Asset..."
curl -d @create-ohdsi-asset.json \
  -H 'content-type: application/json' http://localhost:19193/management/v3/assets \
  -s | jq

echo -e "\n2. Creating OHDSI Open Policy..."
curl -d @create-ohdsi-policy.json \
  -H 'content-type: application/json' http://localhost:19193/management/v3/policydefinitions \
  -s | jq

echo -e "\n3. Creating Contract Definition..."
curl -d @create-ohdsi-contract.json \
  -H 'content-type: application/json' http://localhost:19193/management/v3/contractdefinitions \
  -s | jq
