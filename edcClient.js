const CONSUMER_MANAGEMENT_URL = "http://localhost:29193/management/v3";
const PROVIDER_PROTOCOL_URL = "http://localhost:19194/protocol";
const ASSET_ID = "ohdsi-webapi-v2";

// In-memory cache to avoid negotiating multiple contracts in parallel.
// We store the EDR token PROMISE associated with each endpoint to multiplex concurrency.
let edrPromiseCache = {};

async function fetchWithRetry(url, options, maxRetries = 30, delayMs = 1000) {
  for (let i = 0; i < maxRetries; i++) {
    const res = await fetch(url, options);
    const data = await res.json();
    if (data.state === "FINALIZED" || data.state === "STARTED") {
      return data;
    }
    await new Promise((resolve) => setTimeout(resolve, delayMs));
  }
  throw new Error(`Timeout waiting for EDC operation at ${url}`);
}

async function requestFederatedData(pathSuffix, method = "GET", body = null) {
  console.log(`[EDC CLIENT] Request for path: ${pathSuffix}`);

  if (!edrPromiseCache[ASSET_ID]) {
    console.log(`[EDC CLIENT] Starting full EDC flow (Caching promise)...`);
    edrPromiseCache[ASSET_ID] = negotiateAndGetEdr().catch(err => {
        delete edrPromiseCache[ASSET_ID];
        throw err;
    });
  } else {
    console.log(`[EDC CLIENT] Using cached / in-progress EDC tunnel...`);
  }

  const edr = await edrPromiseCache[ASSET_ID];

  // Make Request to Provider Data Plane
  const targetUrl = `${edr.endpoint}${pathSuffix}`;
  console.log(`[EDC CLIENT] Executing remote Data Pull: ${method} ${targetUrl}`);
  
  const fetchOptions = {
      method: method,
      headers: { "Authorization": edr.authorization }
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

async function negotiateAndGetEdr() {
    // 1. Fetch Catalog
    console.log(`[EDC CLIENT] 1. Requesting Catalog...`);
    const catalogRes = await fetch(`${CONSUMER_MANAGEMENT_URL}/catalog/request`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        "@context": { "@vocab": "https://w3id.org/edc/v0.0.1/ns/" },
        counterPartyAddress: PROVIDER_PROTOCOL_URL,
        protocol: "dataspace-protocol-http"
      })
    });
    const catalog = await catalogRes.json();
    
    // Find our asset in the catalog
    let dataset = catalog["dcat:dataset"];
    if (Array.isArray(dataset)) {
       dataset = dataset.find(d => d["@id"] === ASSET_ID);
    }
    if (!dataset || dataset["@id"] !== ASSET_ID) {
        throw new Error("Asset not found in catalog");
    }
    
    const policy = dataset["odrl:hasPolicy"];
    const offerId = policy["@id"];

    // 2. Negotiate Contract
    console.log(`[EDC CLIENT] 2. Negotiating Contract (Offer: ${offerId})...`);
    const negRes = await fetch(`${CONSUMER_MANAGEMENT_URL}/contractnegotiations`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        "@context": { "@vocab": "https://w3id.org/edc/v0.0.1/ns/" },
        "@type": "ContractRequest",
        counterPartyAddress: PROVIDER_PROTOCOL_URL,
        protocol: "dataspace-protocol-http",
        policy: {
          "@context": "http://www.w3.org/ns/odrl.jsonld",
          "@id": offerId,
          "@type": "Offer",
          assigner: "provider",
          target: ASSET_ID
        }
      })
    });
    const negData = await negRes.json();
    const negotiationId = negData["@id"];

    // 3. Poll Negotiation Status
    console.log(`[EDC CLIENT] 3. Waiting for negotiation FINALIZED ${negotiationId}...`);
    const finalNeg = await fetchWithRetry(`${CONSUMER_MANAGEMENT_URL}/contractnegotiations/${negotiationId}`);
    const contractAgreementId = finalNeg.contractAgreementId;

    // 4. Start Transfer
    console.log(`[EDC CLIENT] 4. Starting Transfer (Agreement: ${contractAgreementId})...`);
    const transRes = await fetch(`${CONSUMER_MANAGEMENT_URL}/transferprocesses`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        "@context": { "@vocab": "https://w3id.org/edc/v0.0.1/ns/" },
        "@type": "TransferRequestDto",
        connectorId: "provider",
        counterPartyAddress: PROVIDER_PROTOCOL_URL,
        contractId: contractAgreementId,
        protocol: "dataspace-protocol-http",
        transferType: "HttpData-PULL"
      })
    });
    const transData = await transRes.json();
    const transferProcessId = transData["@id"];

    // 5. Poll Transfer Status
    console.log(`[EDC CLIENT] 5. Waiting for transfer STARTED ${transferProcessId}...`);
    await fetchWithRetry(`${CONSUMER_MANAGEMENT_URL}/transferprocesses/${transferProcessId}`);

  // 6. Get EDR Token
  console.log(`[EDC CLIENT] 6. Obtaining EDR Token...`);
  const edrRes = await fetch(`${CONSUMER_MANAGEMENT_URL}/edrs/${transferProcessId}/dataaddress`);
  const edr = await edrRes.json();
  
  return edr;
}

module.exports = {
  requestFederatedData
};
