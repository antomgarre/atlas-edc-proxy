// ---------------------------------------------------------------------------
// EDC Client — Handles the full Dataspace Protocol flow:
//   Catalog → Contract Negotiation → Transfer → EDR Token → Data Pull
//
// Compatible with Eclipse EDC Minimum Viable Dataspace (MVD) connectors
// deployed on Kubernetes with DID-based identity.
// ---------------------------------------------------------------------------

// Configuration — all overridable via environment variables.
const CONSUMER_MANAGEMENT_URL =
  process.env.EDC_CONSUMER_MANAGEMENT_URL ||
  "http://127.0.0.1:8081/consumer/cp/api/management/v3";

const PROVIDER_PROTOCOL_URL =
  process.env.EDC_PROVIDER_PROTOCOL_URL ||
  "http://provider-qna-controlplane:8082/api/dsp";

const PROVIDER_ID =
  process.env.EDC_PROVIDER_ID ||
  "did:web:provider-identityhub%3A7083:provider";

const EDC_API_KEY = process.env.EDC_API_KEY || "password";

const ASSET_ID = process.env.EDC_ASSET_ID || "ohdsi-webapi-v3";

// When the proxy runs OUTSIDE the K8s cluster, the EDR data-plane endpoint
// (e.g. http://provider-qna-dataplane:11002/api/public) is unreachable.
// Set this to a port-forwarded or ingress URL so the proxy can reach it.
const PUBLIC_ENDPOINT_OVERRIDE = process.env.EDC_PUBLIC_ENDPOINT_OVERRIDE || "";

// ---------------------------------------------------------------------------
// In-memory cache: we store the EDR token PROMISE per asset to avoid
// negotiating multiple contracts in parallel.
// ---------------------------------------------------------------------------
let edrPromiseCache = {};

// ---------------------------------------------------------------------------
// Helper: common headers for all management API calls
// ---------------------------------------------------------------------------
function managementHeaders() {
  return {
    "Content-Type": "application/json",
    "x-api-key": EDC_API_KEY,
  };
}

// ---------------------------------------------------------------------------
// Helper: poll an EDC endpoint until state is FINALIZED or STARTED
// ---------------------------------------------------------------------------
async function fetchWithRetry(url, maxRetries = 30, delayMs = 1000) {
  for (let i = 0; i < maxRetries; i++) {
    const res = await fetch(url, { headers: managementHeaders() });
    const data = await res.json();
    if (data.state === "FINALIZED" || data.state === "STARTED") {
      return data;
    }
    if (data.state === "TERMINATED") {
      throw new Error(
        `EDC operation TERMINATED: ${data.errorDetail || "unknown reason"}`
      );
    }
    await new Promise((resolve) => setTimeout(resolve, delayMs));
  }
  throw new Error(`Timeout waiting for EDC operation at ${url}`);
}

// ---------------------------------------------------------------------------
// Public API: request data from the federated provider via EDC data plane
// ---------------------------------------------------------------------------
async function requestFederatedData(pathSuffix, method = "GET", body = null) {
  console.log(`[EDC CLIENT] Request for path: ${pathSuffix}`);

  if (!edrPromiseCache[ASSET_ID]) {
    console.log(`[EDC CLIENT] Starting full EDC flow (Caching promise)...`);
    edrPromiseCache[ASSET_ID] = negotiateAndGetEdr().catch((err) => {
      delete edrPromiseCache[ASSET_ID];
      throw err;
    });
  } else {
    console.log(`[EDC CLIENT] Using cached / in-progress EDC tunnel...`);
  }

  const edr = await edrPromiseCache[ASSET_ID];

  // Resolve the data-plane endpoint (override if necessary)
  let endpoint = edr.endpoint;
  if (PUBLIC_ENDPOINT_OVERRIDE) {
    endpoint = PUBLIC_ENDPOINT_OVERRIDE;
  }

  const targetUrl = `${endpoint}${pathSuffix}`;
  console.log(
    `[EDC CLIENT] Executing remote Data Pull: ${method} ${targetUrl}`
  );

  const fetchOptions = {
    method: method,
    headers: { Authorization: edr.authorization },
  };
  if (body) {
    fetchOptions.body = JSON.stringify(body);
    fetchOptions.headers["Content-Type"] = "application/json";
  }

  const result = await fetch(targetUrl, fetchOptions);

  if (!result.ok) {
    if (result.status === 401 || result.status === 403) {
      console.warn(`[EDC CLIENT] EDR Token likely expired. Clearing cache.`);
      delete edrPromiseCache[ASSET_ID];
    }
    throw new Error(`EDC Data Plane responded with status ${result.status}`);
  }
  return await result.json();
}

// ---------------------------------------------------------------------------
// Core: full EDC Dataspace Protocol negotiation pipeline
// ---------------------------------------------------------------------------
async function negotiateAndGetEdr() {
  // 1. Fetch Catalog
  console.log(`[EDC CLIENT] 1. Requesting Catalog from ${PROVIDER_ID}...`);
  const catalogRes = await fetch(
    `${CONSUMER_MANAGEMENT_URL}/catalog/request`,
    {
      method: "POST",
      headers: managementHeaders(),
      body: JSON.stringify({
        "@context": { "@vocab": "https://w3id.org/edc/v0.0.1/ns/" },
        counterPartyAddress: PROVIDER_PROTOCOL_URL,
        counterPartyId: PROVIDER_ID,
        protocol: "dataspace-protocol-http",
      }),
    }
  );
  const catalog = await catalogRes.json();

  // Find our asset in the catalog
  let datasets = catalog["dcat:dataset"];
  if (!Array.isArray(datasets)) datasets = datasets ? [datasets] : [];
  const dataset = datasets.find((d) => d["@id"] === ASSET_ID);
  if (!dataset) {
    throw new Error(
      `Asset '${ASSET_ID}' not found in catalog (${datasets.length} datasets available)`
    );
  }

  const policy = dataset["odrl:hasPolicy"];
  const offerId = policy["@id"];
  console.log(`[EDC CLIENT]    Asset found. Offer: ${offerId}`);

  // 2. Negotiate Contract
  // IMPORTANT: The negotiation MUST include the exact permission/prohibition/obligation
  // arrays from the catalog offer. Omitting them causes a policy mismatch and TERMINATION.
  console.log(`[EDC CLIENT] 2. Negotiating Contract...`);
  const negRes = await fetch(
    `${CONSUMER_MANAGEMENT_URL}/contractnegotiations`,
    {
      method: "POST",
      headers: managementHeaders(),
      body: JSON.stringify({
        "@context": { "@vocab": "https://w3id.org/edc/v0.0.1/ns/" },
        "@type": "ContractRequest",
        counterPartyAddress: PROVIDER_PROTOCOL_URL,
        counterPartyId: PROVIDER_ID,
        protocol: "dataspace-protocol-http",
        policy: {
          "@context": "http://www.w3.org/ns/odrl.jsonld",
          "@id": offerId,
          "@type": "Offer",
          assigner: PROVIDER_ID,
          target: ASSET_ID,
          permission: policy["odrl:permission"]
            ? Array.isArray(policy["odrl:permission"])
              ? policy["odrl:permission"]
              : [policy["odrl:permission"]]
            : [],
          prohibition: policy["odrl:prohibition"] || [],
          obligation: policy["odrl:obligation"] || [],
        },
      }),
    }
  );
  const negData = await negRes.json();
  if (negData["@type"] !== "IdResponse") {
    throw new Error(
      `Contract negotiation initiation failed: ${JSON.stringify(negData)}`
    );
  }
  const negotiationId = negData["@id"];

  // 3. Poll Negotiation Status
  console.log(
    `[EDC CLIENT] 3. Waiting for negotiation FINALIZED (${negotiationId})...`
  );
  const finalNeg = await fetchWithRetry(
    `${CONSUMER_MANAGEMENT_URL}/contractnegotiations/${negotiationId}`
  );
  const contractAgreementId = finalNeg.contractAgreementId;
  console.log(`[EDC CLIENT]    Agreement: ${contractAgreementId}`);

  // 4. Start Transfer
  console.log(`[EDC CLIENT] 4. Starting Transfer...`);
  const transRes = await fetch(
    `${CONSUMER_MANAGEMENT_URL}/transferprocesses`,
    {
      method: "POST",
      headers: managementHeaders(),
      body: JSON.stringify({
        "@context": { "@vocab": "https://w3id.org/edc/v0.0.1/ns/" },
        "@type": "TransferRequestDto",
        connectorId: PROVIDER_ID,
        counterPartyAddress: PROVIDER_PROTOCOL_URL,
        contractId: contractAgreementId,
        protocol: "dataspace-protocol-http",
        transferType: "HttpData-PULL",
      }),
    }
  );
  const transData = await transRes.json();
  const transferProcessId = transData["@id"];

  // 5. Poll Transfer Status
  console.log(
    `[EDC CLIENT] 5. Waiting for transfer STARTED (${transferProcessId})...`
  );
  await fetchWithRetry(
    `${CONSUMER_MANAGEMENT_URL}/transferprocesses/${transferProcessId}`
  );

  // 6. Get EDR Token
  console.log(`[EDC CLIENT] 6. Obtaining EDR Token...`);
  const edrRes = await fetch(
    `${CONSUMER_MANAGEMENT_URL}/edrs/${transferProcessId}/dataaddress`,
    { headers: managementHeaders() }
  );
  const edr = await edrRes.json();

  console.log(
    `[EDC CLIENT] ✅ EDC tunnel established! Endpoint: ${edr.endpoint}`
  );
  return edr;
}

module.exports = {
  requestFederatedData,
  // Export config getters for server.js startup log
  getConfig: () => ({
    consumerManagementUrl: CONSUMER_MANAGEMENT_URL,
    providerProtocolUrl: PROVIDER_PROTOCOL_URL,
    providerId: PROVIDER_ID,
    assetId: ASSET_ID,
    publicEndpointOverride: PUBLIC_ENDPOINT_OVERRIDE || "(none)",
  }),
};
