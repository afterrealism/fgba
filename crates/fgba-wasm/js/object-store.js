const DEFAULT_MAX_RANGE_BYTES = 8 * 1024 * 1024;
const MAX_REQUESTS = 128;

function objectError(code, message, status) {
  const error = new Error(message);
  error.name = code;
  error.code = code;
  if (status !== undefined) error.status = status;
  return error;
}

function asBytes(value) {
  if (value instanceof Uint8Array) return value;
  if (value instanceof ArrayBuffer) return new Uint8Array(value);
  if (ArrayBuffer.isView(value)) return new Uint8Array(value.buffer, value.byteOffset, value.byteLength);
  throw objectError("ObjectResponseError", "Object response body is not binary data");
}

export function parseRangeHeader(rangeHeader, maxRangeBytes = DEFAULT_MAX_RANGE_BYTES) {
  const match = /^bytes=(0|[1-9]\d*)-(0|[1-9]\d*)$/.exec(rangeHeader || "");
  if (!match) throw objectError("InvalidRange", "Only explicit byte ranges are accepted");
  const start = BigInt(match[1]);
  const end = BigInt(match[2]);
  if (end < start) throw objectError("InvalidRange", "Byte range is inverted");
  const len = end - start + 1n;
  if (len > BigInt(maxRangeBytes)) {
    throw objectError("RangeLimit", `Byte range exceeds the ${maxRangeBytes}-byte cap`);
  }
  return { start, end, len };
}

export function parseContentRange(contentRange) {
  const match = /^bytes (0|[1-9]\d*)-(0|[1-9]\d*)\/(0|[1-9]\d*)$/.exec((contentRange || "").trim());
  if (!match) throw objectError("ContentRangeError", "Response has an invalid Content-Range");
  const start = BigInt(match[1]);
  const end = BigInt(match[2]);
  const total = BigInt(match[3]);
  if (end < start || total <= end) {
    throw objectError("ContentRangeError", "Response Content-Range is inconsistent");
  }
  return { start, end, total };
}

function validateRangeHeaders(response, rangeHeader, maxRangeBytes) {
  const requested = parseRangeHeader(rangeHeader, maxRangeBytes);
  if (response.status === 404) throw objectError("ObjectNotFound", "Object does not exist", 404);
  if (response.status === 200) {
    throw objectError("RangeRequired", "Object store returned 200 to a partial read; 206 is required", 200);
  }
  if (response.status !== 206) {
    throw objectError("ObjectResponseError", `Object store returned status ${response.status}`, response.status);
  }
  const actual = parseContentRange(response.contentRange);
  if (actual.start !== requested.start || actual.end !== requested.end) {
    throw objectError("ContentRangeError", "Response Content-Range does not match the requested span");
  }
  if (response.contentLength !== undefined && response.contentLength !== null && response.contentLength !== "") {
    if (!/^(0|[1-9]\d*)$/.test(String(response.contentLength)) || BigInt(response.contentLength) !== requested.len) {
      throw objectError("ShortRead", "Object response Content-Length does not match the requested span");
    }
  }
  return requested;
}

export function validateRangeResponse(response, rangeHeader, maxRangeBytes = DEFAULT_MAX_RANGE_BYTES) {
  const requested = validateRangeHeaders(response, rangeHeader, maxRangeBytes);
  const bytes = asBytes(response.bytes);
  if (BigInt(bytes.byteLength) !== requested.len) {
    throw objectError("ShortRead", "Object response body length does not match the requested span");
  }
  return {
    status: response.status,
    bytes,
    contentRange: response.contentRange,
    contentLength: String(bytes.byteLength),
  };
}

async function readBoundedBody(response, expectedLength) {
  if (!response.body?.getReader) {
    throw objectError("ObjectTransportError", "Object response body streaming is unavailable");
  }

  const reader = response.body.getReader();
  const bytes = new Uint8Array(expectedLength);
  let offset = 0;
  let complete = false;
  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      if (value.byteLength > expectedLength - offset) {
        throw objectError("ShortRead", "Object response exceeds the requested span");
      }
      bytes.set(value, offset);
      offset += value.byteLength;
    }
    complete = true;
    return bytes.subarray(0, offset);
  } catch (error) {
    if (error?.code || error?.name === "AbortError") throw error;
    throw objectError("ObjectTransportError", "Object response body could not be read");
  } finally {
    if (!complete) await reader.cancel().catch(() => {});
    reader.releaseLock();
  }
}

function signingMetadata(value, requestedKey, expectedGeneration, onGenerationChange) {
  if (!value || !Array.isArray(value.objects) || value.objects.length !== 1) {
    throw objectError("SigningFailed", "Signing response must contain exactly one object");
  }
  const [signedObject] = value.objects;
  if (signedObject.path !== requestedKey || typeof signedObject.url !== "string" || signedObject.url.length === 0) {
    throw objectError("SigningFailed", "Signing response object does not match the requested path");
  }
  let signedUrl;
  try {
    signedUrl = new URL(signedObject.url);
  } catch {
    throw objectError("SigningFailed", "Signing response URL is invalid");
  }
  const loopbackHttp = signedUrl.protocol === "http:"
    && (["localhost", "127.0.0.1", "[::1]"].includes(signedUrl.hostname)
      || signedUrl.hostname.endsWith(".localhost"));
  if ((signedUrl.protocol !== "https:" && !loopbackHttp) || signedUrl.username || signedUrl.password) {
    throw objectError("SigningFailed", "Signing response URL must be HTTPS or loopback HTTP without credentials");
  }
  if (value.generation === undefined || value.generation === null) {
    throw objectError("SigningFailed", "Signing response did not include a generation");
  }
  const returnedGeneration = String(value.generation);
  if (returnedGeneration !== expectedGeneration) {
    onGenerationChange(returnedGeneration);
    throw objectError("GenerationChanged", "Object generation changed; cached dataset state was discarded");
  }
  const now = Date.now();
  const numericExpiry = typeof value.expires_at === "number";
  if (numericExpiry && !Number.isSafeInteger(value.expires_at)) {
    throw objectError("SigningFailed", "Signing response expiry must be an integer Unix timestamp");
  }
  const expiresAt = numericExpiry ? value.expires_at * 1000 : Date.parse(value.expires_at);
  if (!Number.isFinite(expiresAt) || !Number.isSafeInteger(expiresAt) || expiresAt <= now) {
    throw objectError("SigningFailed", "Signing response has an invalid expiry");
  }
  const ttl = expiresAt - now;
  const skew = Math.min(60_000, ttl * 0.2);
  return { url: signedObject.url, refreshAt: expiresAt - skew, generation: returnedGeneration };
}

/**
 * Create the async transport consumed by FgbaDataset. The get callback never
 * exposes the signed URL and accepts only {key, rangeHeader} (plus an optional
 * AbortSignal for cancellation).
 */
export function createSignedObjectStore({
  key,
  signEndpoint,
  generation,
  maxRangeBytes = DEFAULT_MAX_RANGE_BYTES,
  fetchImpl = globalThis.fetch,
  onGenerationChange = () => {},
}) {
  if (typeof key !== "string" || key.length === 0) throw new TypeError("key is required");
  if (generation === undefined || generation === null) throw new TypeError("generation is required");
  if (typeof fetchImpl !== "function") throw new TypeError("fetch is required");
  const expectedGeneration = String(generation);
  let signed = null;
  let refreshing = null;
  let remainingRequests = 0;
  let invalidated = false;

  function invalidate(nextGeneration) {
    invalidated = true;
    signed = null;
    onGenerationChange(nextGeneration);
  }

  function consumeRequest() {
    if (remainingRequests <= 0) {
      throw objectError("RequestLimit", `Operation exceeds the ${MAX_REQUESTS}-request cap`);
    }
    remainingRequests -= 1;
  }

  async function refresh(force, signal) {
    if (invalidated) throw objectError("GenerationChanged", "Object generation changed; dataset is closed");
    if (!force && signed && Date.now() < signed.refreshAt) return signed;
    if (refreshing) return refreshing;
    refreshing = (async () => {
      let metadata;
      try {
        if (typeof signEndpoint === "function") {
          metadata = await signEndpoint({ key, generation: expectedGeneration, signal });
        } else {
          const response = await fetchImpl(signEndpoint, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ paths: [key] }),
            credentials: "omit",
            redirect: "error",
            cache: "no-store",
            signal,
          });
          if (response.status !== 200) {
            throw objectError("SigningFailed", `Signing endpoint returned status ${response.status}`, response.status);
          }
          metadata = await response.json();
        }
      } catch (error) {
        if (error?.code === "SigningFailed" || error?.name === "AbortError") throw error;
        throw objectError("SigningFailed", "Could not refresh object authorization");
      }
      const next = signingMetadata(metadata, key, expectedGeneration, invalidate);
      signed = next;
      return next;
    })();
    try {
      return await refreshing;
    } finally {
      refreshing = null;
    }
  }

  return {
    beginOperation(limit = MAX_REQUESTS) {
      if (!Number.isSafeInteger(limit) || limit <= 0 || limit > MAX_REQUESTS) {
        throw new TypeError(`operation request limit must be in 1..=${MAX_REQUESTS}`);
      }
      remainingRequests = limit;
    },

    invalidate() {
      invalidated = true;
      signed = null;
    },

    async get({ key: requestedKey, rangeHeader, signal }) {
      if (requestedKey !== key) throw objectError("ObjectKeyMismatch", "Object key does not match this dataset");
      if (invalidated) throw objectError("GenerationChanged", "Object generation changed; dataset is closed");
      parseRangeHeader(rangeHeader, maxRangeBytes);
      let refreshedAfterAuth = false;
      for (;;) {
        const authorization = await refresh(refreshedAfterAuth, signal);
        consumeRequest();
        let response;
        try {
          response = await fetchImpl(authorization.url, {
            method: "GET",
            headers: { Range: rangeHeader },
            credentials: "omit",
            redirect: "error",
            cache: "no-store",
            signal,
          });
        } catch (error) {
          if (error?.name === "AbortError") throw error;
          throw objectError("ObjectTransportError", "Object request failed before a response was received");
        }
        if ((response.status === 401 || response.status === 403) && !refreshedAfterAuth) {
          refreshedAfterAuth = true;
          signed = null;
          continue;
        }
        const metadata = {
          status: response.status,
          contentRange: response.headers.get("Content-Range"),
          contentLength: response.headers.get("Content-Length"),
        };
        let requested;
        try {
          requested = validateRangeHeaders(metadata, rangeHeader, maxRangeBytes);
        } catch (error) {
          if (response.body) await response.body.cancel().catch(() => {});
          throw error;
        }
        let bytes;
        try {
          bytes = await readBoundedBody(response, Number(requested.len));
        } catch (error) {
          if (error?.code || error?.name === "AbortError") throw error;
          throw objectError("ObjectTransportError", "Object response body could not be read");
        }
        return validateRangeResponse({ ...metadata, bytes }, rangeHeader, maxRangeBytes);
      }
    },
  };
}
