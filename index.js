const WebSocket = require("ws");
const protobuf = require("protobufjs");
const path = require("path");
require("dotenv").config();

const WS_URL = "wss://stream.kairos.trade";

const TAGS = {
  // Server -> Client
  0x01: "Tick",
  0x02: "OrderbookSnapshot",
  0x03: "Candle",
  0x04: "TradeBatch",
  0x05: "PriceUpdate",
  0x06: "Volume",
  0x0f: "OrderbookDelta",
  0x13: "PongResponse",
  0x14: "SubscribedResponse",
  0x15: "UnsubscribedResponse",
  0x17: "ErrorResponse",
  0x18: "ServerPingRequest",
};

const CLIENT_TAGS = {
  SubscribeRequest: 0x10,
  UnsubscribeRequest: 0x11,
  PingRequest: 0x12,
  ClientPongResponse: 0x19,
  FetchRequest: 0x16,
};

const SIDE_LABELS = { 0: "???", 1: "BUY", 2: "SELL" };
const PROVIDER_LABELS = { 0: "unknown", 1: "polymarket", 2: "kalshi" };

function formatPrice(scaled) {
  return (scaled / 10000).toFixed(4);
}

function formatTime(unixSec) {
  return new Date(unixSec * 1000).toISOString().replace("T", " ").slice(0, 19);
}

async function main() {
  const contractId = process.argv[2];
  const provider = process.argv[3] || "polymarket";

  if (!contractId) {
    console.error("Usage: node index.js <contract_id> [provider]");
    console.error("  provider: polymarket (default) or kalshi");
    process.exit(1);
  }

  const clientId = process.env.KAIROS_CLIENT_ID;
  const apiKey = process.env.KAIROS_API_KEY;
  const apiSecret = process.env.KAIROS_API_SECRET || process.env.KAIROS_CLIENT_SECRET;

  if (!clientId || !apiKey || !apiSecret) {
    console.error("Missing credentials. Set KAIROS_CLIENT_ID, KAIROS_API_KEY, KAIROS_API_SECRET in .env");
    process.exit(1);
  }

  const root = await protobuf.load(path.join(__dirname, "kairos.proto"));
  const types = {};
  for (const name of Object.values(TAGS)) {
    types[name] = root.lookupType(`kairos.${name}`);
  }
  for (const name of Object.keys(CLIENT_TAGS)) {
    types[name] = root.lookupType(`kairos.${name}`);
  }

  console.log(`Connecting to ${WS_URL}...`);

  const ws = new WebSocket(WS_URL, {
    headers: {
      "X-Client-Id": clientId,
      "X-Api-Key": apiKey,
      "X-Api-Secret": apiSecret,
    },
  });

  function send(msgName, payload) {
    const tag = CLIENT_TAGS[msgName];
    const msgType = types[msgName];
    const encoded = msgType.encode(msgType.create(payload)).finish();
    const frame = Buffer.alloc(1 + encoded.length);
    frame[0] = tag;
    encoded.copy(frame, 1);
    ws.send(frame);
  }

  let tradeCount = 0;

  ws.on("open", () => {
    console.log("Connected. Subscribing to all topics...\n");
    send("SubscribeRequest", {
      contractId: contractId,
      provider: provider,
      topics: [],
    });
  });

  ws.on("message", (data) => {
    const buf = Buffer.from(data);
    if (buf.length < 2) return;

    const tag = buf[0];
    const payload = buf.slice(1);
    const msgName = TAGS[tag];

    if (!msgName) return; // unknown tag, ignore per spec

    const msgType = types[msgName];
    if (!msgType) return;

    let decoded;
    try {
      decoded = msgType.decode(payload);
    } catch (e) {
      console.error(`Failed to decode ${msgName}:`, e.message);
      return;
    }

    switch (tag) {
      case 0x18: // ServerPingRequest
        send("ClientPongResponse", { timestampUs: decoded.timestampUs });
        break;

      case 0x14: // SubscribedResponse
        console.log(`Subscribed to contract: ${decoded.contractId}`);
        console.log("Waiting for data...\n");
        break;

      case 0x17: // ErrorResponse
        console.error(`Server error [${decoded.code}]: ${decoded.message}`);
        break;

      case 0x04: // TradeBatch
        for (const trade of decoded.trades) {
          tradeCount++;
          const time = formatTime(Number(trade.timestampSec));
          const side = (SIDE_LABELS[trade.side] || "???").padEnd(6);
          const price = formatPrice(trade.price).padStart(8);
          const size = String(trade.sizeScaled).padStart(12);
          const outcome = (trade.outcome || `idx:${trade.outcomeIndex}`).padEnd(16);
          const id = trade.tradeId || "-";
          console.log(`[TRADE]  ${time}  ${side} ${price} ${size}  ${outcome} ${id}`);
        }
        break;

      case 0x05: // PriceUpdate
        console.log(`[PRICE]  contract=${decoded.contractId}  mid=${formatPrice(decoded.midPrice)}  bid=${formatPrice(decoded.bestBid)}  ask=${formatPrice(decoded.bestAsk)}`);
        break;

      case 0x06: // Volume
        console.log(`[VOL]    contract=${decoded.contractId}  1h=${decoded.volume1hScaled}  24h=${decoded.volume24hScaled}`);
        break;

      case 0x02: // OrderbookSnapshot
        console.log(`[BOOK]   contract=${decoded.contractId}  outcomes=${decoded.outcomeNames.join(",")}  seq=${decoded.seq}`);
        break;

      case 0x0f: // OrderbookDelta
        console.log(`[DELTA]  contract=${decoded.contractId}  seq=${decoded.seq}`);
        break;

      case 0x03: // Candle
        console.log(`[CANDLE] contract=${decoded.contractId}  tf=${decoded.timeframeSeconds}s  vol=${decoded.volumeScaled}`);
        break;

      case 0x01: // Tick
        console.log(`[TICK]   contract=${decoded.contractId}`);
        break;

      case 0x13: // PongResponse
        break;

      default:
        console.log(`[${msgName}] (tag=0x${tag.toString(16)})`);
        break;
    }
  });

  ws.on("close", (code, reason) => {
    console.log(`\nDisconnected (code=${code}). Total trades received: ${tradeCount}`);
    process.exit(0);
  });

  ws.on("error", (err) => {
    console.error("WebSocket error:", err.message);
    process.exit(1);
  });

  process.on("SIGINT", () => {
    console.log(`\nShutting down. Total trades received: ${tradeCount}`);
    ws.close();
  });
}

main().catch((err) => {
  console.error("Fatal:", err);
  process.exit(1);
});
