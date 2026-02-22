const express = require("express");
const { createProxyMiddleware, fixRequestBody } = require("http-proxy-middleware");
const cors = require("cors");
const { requestFederatedData } = require("./edcClient");

const app = express();
const PORT = process.env.PORT || 3000;

// Local WebAPI Configuration (Master)
// Default assumes standard Broadsea deployment on port 8080
const LOCAL_WEBAPI_URL =
  process.env.LOCAL_WEBAPI_URL || "http://localhost:8080";

// Simulated Federated Nodes (Mock Data)
// These would represent the "Contracts" or "Data Assets" retrieved from the EDC Federated Catalog
const FEDERATED_NODES = [
  {
    sourceId: 1001,
    sourceName: "Hospital La Paz (Federated)",
    sourceDialect: "postgresql",
    sourceKey: "hPaz",
    daimons: [
      { daimonType: "CDM", tableQualifier: "cdm", priority: 1 },
      { daimonType: "Vocabulary", tableQualifier: "vocab", priority: 1 },
      { daimonType: "Results", tableQualifier: "results", priority: 1 },
    ],
  },
  {
    sourceId: 1002,
    sourceName: "Hospital Clínico (Federated)",
    sourceDialect: "postgresql",
    sourceKey: "hClinico",
    daimons: [
      { daimonType: "CDM", tableQualifier: "cdm", priority: 1 },
      { daimonType: "Vocabulary", tableQualifier: "vocab", priority: 1 },
      { daimonType: "Results", tableQualifier: "results", priority: 1 },
    ],
  },
];

app.use(cors()); // Allow requests from Atlas frontend
app.use(express.json({ limit: '50mb' })); // Increased payload limit to support large JSON structures typical in cohort characterizations
app.use(express.urlencoded({ limit: '50mb', extended: true }));

// ============================================================================
// MODULE 1: Source Interception
// Intercepts requests for available data sources and merges local WebAPI 
// sources with simulated federated environments.
// ============================================================================
app.get("/WebAPI/source/sources", async (req, res) => {
  try {
    console.log(`[PROXY] Intercepting call to /source/sources...`);

    // 1. Get real sources from local WebAPI (master)
    // Using native Node.js fetch (requires Node >= 18)
    const localSourcesRes = await fetch(
      `${LOCAL_WEBAPI_URL}/WebAPI/source/sources`,
    );
    let localSources = [];
    if (localSourcesRes.ok) {
      localSources = await localSourcesRes.json();
    } else {
      console.warn(
        `[PROXY] Could not connect to local WebAPI (${LOCAL_WEBAPI_URL}). Serving only federated mock.`,
      );
    }

    // 2. Combine local sources with simulated federated nodes
    const combinedSources = [...localSources, ...FEDERATED_NODES];

    console.log(
      `[PROXY] Returning ${combinedSources.length} sources in total (${FEDERATED_NODES.length} injected).`,
    );
    res.json(combinedSources);
  } catch (error) {
    console.error("[PROXY] Error fetching sources:", error);
    res.status(500).json({ error: "Error querying proxy sources" });
  }
});

// ============================================================================
// MODULE 2: Federated Phenotype Library (Cohort Definitions)
// ============================================================================
app.get("/WebAPI/cohortdefinition", async (req, res, next) => {
  try {
    console.log(`[PROXY] Intercepting call to /cohortdefinition (Unified list)...`);

    // 1. Get local cohorts
    const localRes = await fetch(`${LOCAL_WEBAPI_URL}/WebAPI/cohortdefinition`);
    let localCohorts = [];
    if (localRes.ok) {
      localCohorts = await localRes.json();
    } else {
       console.warn(`[PROXY] Could not fetch real cohorts (${localRes.status}).`);
    }

    // 2. Get cohorts from each federated node
    let federatedCohorts = [];
    for (const node of FEDERATED_NODES) {
      try {
        console.log(`[PROXY] Requesting cohorts from federated node: ${node.sourceName}`);
        const remoteCohorts = await requestFederatedData("/cohortdefinition", "GET");
        
        const namespaced = remoteCohorts.map(c => ({
          ...c,
          id: node.sourceId * 1000000 + c.id,
          name: `[${node.sourceName}] ${c.name}`
        }));
        federatedCohorts = federatedCohorts.concat(namespaced);
      } catch (e) {
         console.warn(`[PROXY] Failed to fetch cohorts from ${node.sourceName}: ${e.message}`);
      }
    }

    return res.json([...localCohorts, ...federatedCohorts]);
  } catch (e) {
    console.error("[PROXY] Error in unified /cohortdefinition:", e);
    next();
  }
});

app.get("/WebAPI/cohortdefinition/:id", async (req, res, next) => {
  const idStr = req.params.id;
  // Only intercept if it's an exact number to avoid stepping on subroutes like /info or /generate
  if (!/^\d+$/.test(idStr)) return next();
  
  const id = parseInt(idStr, 10);
  
  if (id < 1000000000) {
     // Local cohort: let it pass transparently to real WebAPI
     return next();
  }

  try {
    const sourceId = Math.floor(id / 1000000);
    const originalId = id % 1000000;
    const node = FEDERATED_NODES.find(n => n.sourceId === sourceId);

    if (!node) {
       return res.status(404).json({error: "Federated Node not found"});
    }

    console.log(`[PROXY] Intercepting fetch of federated cohort. Original ID: ${originalId} from ${node.sourceName}`);
    
    // Request real JSON payload (characterization or algorithm) via EDC tunnel
    const remoteDef = await requestFederatedData(`/cohortdefinition/${originalId}`, "GET");
    
    // Override visual metadata for unified UI
    remoteDef.id = id;
    remoteDef.name = `[${node.sourceName}] ${remoteDef.name}`;
    
    return res.json(remoteDef);
  } catch (e) {
    console.error(`[PROXY] Error fetching federated cohort detail ${idStr}`, e);
    return res.status(500).json({error: "Failed to fetch federated cohort definition"});
  }
});

// ============================================================================
// MODULE 3: Cohort Status Interception (info)
// ============================================================================
app.get("/WebAPI/cohortdefinition/:id/info", async (req, res, next) => {
  try {
    const id = req.params.id;
    console.log(`[PROXY] Intercepting call to /cohortdefinition/${id}/info...`);

    // 1. Get real info from master WebAPI
    const realRes = await fetch(`${LOCAL_WEBAPI_URL}/WebAPI/cohortdefinition/${id}/info`);
    let info = [];
    if (realRes.ok) {
      info = await realRes.json();
    } else {
      console.warn(`[PROXY] Could not fetch real info (${realRes.status}).`);
    }
    
    // 2. Inject simulated 'COMPLETE' status for federated nodes
    const fedInfos = FEDERATED_NODES.map(node => ({
      "id": { "cohortDefinitionId": parseInt(id), "sourceId": node.sourceId },
      "startTime": Date.now() - 15000,
      "executionDuration": 1500 + Math.floor(Math.random() * 1000),
      "status": "COMPLETE",
      "isValid": true,
      "isCanceled": false,
      "failMessage": null,
      "personCount": 1000 + Math.floor(Math.random() * 5000),
      "recordCount": 1000 + Math.floor(Math.random() * 5000),
      "createdBy": "admin",
      "ccGenerateId": null,
      "isDemographic": false
    }));

    // Returns mixture of real and simulated information
    return res.json([...info, ...fedInfos]);
  } catch(e) {
    console.error("[PROXY] Error intercepting /info, delegating to real WebAPI...", e);
    next(); // Transparent fallback on error
  }
});

// ============================================================================
// MODULE 4: Dynamic EDC Routing (Data Plane Simulator)
// We intercept any call directed to a specific SourceKey (representing a remote
// node). Typical WebAPI routes include the sourceKey in the URL, e.g.:
// /WebAPI/cohortdefinition/1/generate/hPaz
// /WebAPI/vocabulary/hPaz/search
// ============================================================================

// Middleware to detect calls to federated nodes
app.use("/WebAPI", async (req, res, next) => {
  const urlPath = req.path;
  const method = req.method;

  // Search if any URL contains the SourceKey of our mock nodes
  const targetFedNode = FEDERATED_NODES.find(
    (node) =>
      urlPath.includes(`/${node.sourceKey}`) ||
      urlPath.includes(`${node.sourceKey}/`),
  );

  if (targetFedNode) {
    console.log(`\n[FEDERATION INVOKED] Request ${method} contains SourceKey '${targetFedNode.sourceKey}'`);
    console.log(`[EDC ROUTER] Starting Data Transfer process for node: ${targetFedNode.sourceName}`);

    if (urlPath.includes("/generate/")) {
      console.log(`[MOCK RESPONSE] Delivering simulated JobStatus for cohort generation on ${targetFedNode.sourceKey}`);
      return res.json({
        id: Math.floor(Math.random() * 10000),
        status: "STARTING",
        progress: 0,
        name: `EDC Federation Job on ${targetFedNode.sourceName}`,
      });
    }

    try {
      // Substitute dummy SourceKey (hPaz) with the real hospital's (EUNOMIA)
      // so the remote WebAPI knows how to respond.
      const federatedPath = urlPath.replace(targetFedNode.sourceKey, "EUNOMIA");
      
      const payloadBody = method !== "GET" && method !== "HEAD" ? req.body : null;

      // Invoke EDC Client logic (Negotiation + Transfer Process + Data Plane Pull)
      const remoteData = await requestFederatedData(federatedPath, method, payloadBody);

      return res.json(remoteData);

    } catch (edcError) {
      console.error(`[EDC ERROR] Error in federated transfer:`, edcError);
      return res.status(502).json({
        error: "EDC Federation Failed",
        message: edcError.message
      });
    }

  } else {
    // If it's not a federated node, let the normal proxy send it to Local WebAPI
    next();
  }
});

// ============================================================================
// TRANSPARENT PROXY
// All REST traffic (including queries to Master WebAPI for saving 
// cohort definitions) is forwarded transparently.
// ============================================================================
app.use(
  createProxyMiddleware({
    target: LOCAL_WEBAPI_URL,
    changeOrigin: true,
    logLevel: "debug",
    on: {
      proxyReq: fixRequestBody,
    },
  }),
);

// Start Server
app.listen(PORT, () => {
  console.log("===================================================");
  console.log(`🚀 Atlas-EDC Federation Proxy Initialized!`);
  console.log(`📡 Listening on Port: ${PORT}`);
  console.log(`🏢 Local WebAPI Target: ${LOCAL_WEBAPI_URL}`);
  console.log(`🌐 Injected Federated Nodes (EDC Mocks):`);
  FEDERATED_NODES.forEach((n) =>
    console.log(`   - [${n.sourceKey}] ${n.sourceName}`),
  );
  console.log("===================================================");
  console.log(
    "🔗 To test, configure the Services layer URL in Atlas to: http://localhost:3000/WebAPI/",
  );
});
