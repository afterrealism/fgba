import init, { SparseDataset } from "../pkg/fgba_wasm.js";
import { createSignedObjectStore, parseContentRange } from "./object-store.js";

const MAX_RANGE_BYTES = 8 * 1024 * 1024;
const MAX_CACHE_BYTES = 64 * 1024 * 1024;
const MAX_FEATURES = 50_000;
const MAX_REQUESTS = 128;
let wasmPromise = null;

/** Initialize the web-target WASM explicitly (useful for Node tests and CSP hosts). */
export async function initializeFgbaWasm(moduleOrPath) {
  if (!wasmPromise) {
    wasmPromise = moduleOrPath === undefined ? init() : init({ module_or_path: moduleOrPath });
  }
  try {
    await wasmPromise;
  } catch (error) {
    wasmPromise = null;
    throw error;
  }
}

function normalError(error) {
  if (error instanceof Error) return error;
  if (typeof error === "string") return new Error(error);
  if (error?.message) return new Error(String(error.message));
  return new Error("FGBA operation failed");
}

function abortError() {
  if (typeof DOMException === "function") return new DOMException("Operation was aborted", "AbortError");
  const error = new Error("Operation was aborted");
  error.name = "AbortError";
  return error;
}

function isWouldBlock(error) {
  return error?.name === "WouldBlock" && error?.code === "FGBA_WOULD_BLOCK"
    && /^(0|[1-9]\d*)$/.test(error.offset || "") && /^(0|[1-9]\d*)$/.test(error.len || "");
}

function validateBbox(bbox) {
  if (!Array.isArray(bbox) || bbox.length !== 4 || bbox.some((value) => !Number.isFinite(value))) {
    throw new TypeError("bbox must contain four finite numbers");
  }
  if (bbox[0] > bbox[2] || bbox[1] > bbox[3]) throw new TypeError("bbox is inverted");
  return bbox;
}

function validateLimit(value, maximum, label) {
  if (!Number.isSafeInteger(value) || value <= 0 || value > maximum) {
    throw new TypeError(`${label} must be in 1..=${maximum}`);
  }
  return value;
}

/** Browser FGBA dataset backed only by sparse signed HTTP ranges. */
export class FgbaDataset {
  constructor(options) {
    this.key = options.key;
    this.maxRangeBytes = options.maxRangeBytes;
    this.maxCacheBytes = options.maxCacheBytes;
    this.wasm = null;
    this.bandMetadata = null;
    this.closed = false;
    this.queue = Promise.resolve();
    this.store = createSignedObjectStore({
      key: options.key,
      signEndpoint: options.signEndpoint,
      generation: options.generation,
      maxRangeBytes: options.maxRangeBytes,
      fetchImpl: options.fetchImpl,
      onGenerationChange: () => this.close(),
    });
  }

  /** Open a dataset after a 0-0 probe and explicit footer/directory reads. */
  static async open({
    key,
    signEndpoint,
    generation,
    maxRangeBytes = MAX_RANGE_BYTES,
    maxCacheBytes = MAX_CACHE_BYTES,
    fetchImpl,
    signal,
  }) {
    validateLimit(maxRangeBytes, MAX_RANGE_BYTES, "maxRangeBytes");
    validateLimit(maxCacheBytes, MAX_CACHE_BYTES, "maxCacheBytes");
    await initializeFgbaWasm();
    const dataset = new FgbaDataset({ key, signEndpoint, generation, maxRangeBytes, maxCacheBytes, fetchImpl });
    try {
      dataset.store.beginOperation(MAX_REQUESTS);
      const probe = await dataset.store.get({ key, rangeHeader: "bytes=0-0", signal });
      const { total } = parseContentRange(probe.contentRange);
      dataset.wasm = new SparseDataset(total.toString(), maxRangeBytes, maxCacheBytes);
      await dataset.drive(() => dataset.wasm.open(), signal);
      dataset.bandMetadata = JSON.parse(dataset.wasm.bands());
      return dataset;
    } catch (error) {
      dataset.close();
      throw normalError(error);
    }
  }

  assertActive(signal) {
    if (signal?.aborted) throw abortError();
    if (this.closed || !this.wasm) {
      const error = new Error("Dataset is closed");
      error.name = "DatasetClosed";
      throw error;
    }
  }

  async drive(operation, signal) {
    for (;;) {
      this.assertActive(signal);
      let miss;
      try {
        const result = operation();
        this.assertActive(signal);
        return result;
      } catch (error) {
        if (!isWouldBlock(error)) throw normalError(error);
        miss = error;
      }
      const offset = BigInt(miss.offset);
      const len = BigInt(miss.len);
      if (len === 0n || len > BigInt(this.maxRangeBytes)) throw new Error("WASM requested an invalid range");
      const rangeHeader = `bytes=${offset}-${offset + len - 1n}`;
      const response = await this.store.get({ key: this.key, rangeHeader, signal });
      this.assertActive(signal);
      this.wasm.supplyRange(offset.toString(), response.bytes);
    }
  }

  enqueue(operation) {
    const run = this.queue.then(operation, operation);
    this.queue = run.catch(() => {});
    return run;
  }

  /** Band metadata; offsets and sizes are decimal strings. */
  bands() {
    this.assertActive();
    return this.bandMetadata.map((band) => ({ ...band }));
  }

  /** Query approximate A5 cells. estimatedCount remains a decimal string. */
  async queryA5({ bbox, res, signal }) {
    validateBbox(bbox);
    if (res !== undefined && (!Number.isInteger(res) || res < 0 || res > 30)) {
      throw new TypeError("res must be in 0..=30");
    }
    return this.enqueue(async () => {
      this.assertActive(signal);
      this.store.beginOperation(MAX_REQUESTS);
      const json = await this.drive(() => this.wasm.queryA5(bbox, res), signal);
      return JSON.parse(json);
    });
  }

  /** Query exact FlatGeobuf features, capped at 50,000 rows. */
  async queryFeatures({ bbox, limit = MAX_FEATURES, signal }) {
    validateBbox(bbox);
    if (!Number.isSafeInteger(limit) || limit < 0 || limit > MAX_FEATURES) {
      throw new TypeError(`limit must be in 0..=${MAX_FEATURES}`);
    }
    return this.enqueue(async () => {
      this.assertActive(signal);
      this.store.beginOperation(MAX_REQUESTS);
      const json = await this.drive(() => this.wasm.queryFeatures(bbox, limit), signal);
      return JSON.parse(json);
    });
  }

  /** Immediately invalidate transport and discard all WASM/cache state. */
  close() {
    if (this.closed) return;
    this.closed = true;
    this.store.invalidate();
    this.bandMetadata = null;
    if (this.wasm) {
      this.wasm.close();
      this.wasm.free();
      this.wasm = null;
    }
  }
}
