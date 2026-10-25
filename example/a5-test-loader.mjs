export function resolve(specifier, context, next) {
  if (specifier === "a5-js") {
    return { url: new URL("./vendor/a5.js", import.meta.url).href, shortCircuit: true };
  }
  return next(specifier, context);
}
