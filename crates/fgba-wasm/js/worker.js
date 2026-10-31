import { FgbaDataset } from "./client.js";

let dataset = null;
let queue = Promise.resolve();
let viewId = 0;
let activeAbort = null;

function serializeError(error) {
  return {
    name: String(error?.name || "Error"),
    code: error?.code ? String(error.code) : undefined,
    status: Number.isInteger(error?.status) ? error.status : undefined,
    message: String(error?.message || error || "FGBA worker operation failed"),
  };
}

function post(id, ok, value) {
  if (id === undefined) return;
  if (ok) self.postMessage({ id, ok: true, value });
  else self.postMessage({ id, ok: false, error: serializeError(value) });
}

function staleError() {
  const error = new Error("A newer dataset view superseded this operation");
  error.name = "StaleView";
  return error;
}

function enqueue(operation) {
  const run = queue.then(operation, operation);
  queue = run.catch(() => {});
  return run;
}

function closeCurrent() {
  activeAbort?.abort();
  activeAbort = null;
  dataset?.close();
  dataset = null;
}

self.onmessage = (event) => {
  const { id, type } = event.data || {};
  if (type === "close") {
    viewId += 1;
    closeCurrent();
    enqueue(() => post(id, true, null));
    return;
  }
  if (!["open", "bands", "queryA5", "queryFeatures", "querySql"].includes(type)) {
    post(id, false, new Error(`Unknown worker operation: ${type}`));
    return;
  }

  const requestView = ++viewId;
  activeAbort?.abort();
  const controller = new AbortController();
  activeAbort = controller;
  enqueue(async () => {
    try {
      if (requestView !== viewId) throw staleError();
      let value;
      if (type === "open") {
        dataset?.close();
        dataset = null;
        const opened = await FgbaDataset.open({ ...event.data.options, signal: controller.signal });
        if (requestView !== viewId) {
          opened.close();
          throw staleError();
        }
        dataset = opened;
        value = opened.bands();
      } else {
        if (!dataset) throw new Error("No dataset is open");
        if (type === "bands") value = dataset.bands();
        if (type === "queryA5") value = await dataset.queryA5({ ...event.data.options, signal: controller.signal });
        if (type === "queryFeatures") value = await dataset.queryFeatures({ ...event.data.options, signal: controller.signal });
        if (type === "querySql") value = await dataset.querySql({ ...event.data.options, signal: controller.signal });
      }
      if (requestView !== viewId) throw staleError();
      post(id, true, value);
    } catch (error) {
      post(id, false, error?.name === "AbortError" && requestView !== viewId ? staleError() : error);
    }
  });
};
