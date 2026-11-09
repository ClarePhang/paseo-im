import { createRequire } from "node:module";
import { dirname, isAbsolute, join } from "node:path";
import { pathToFileURL } from "node:url";

/**
 * Load a node_modules package at runtime instead of letting the daemon's
 * server bundler inline it. Needed for SDKs whose code references bare
 * `__dirname` or `import.meta`: the daemon executes the CJS bundle inside a
 * plain function scope, where neither is defined.
 *
 * The specifier is only present as a runtime string, so the bundler keeps
 * this call opaque. Resolution is anchored to the plugin's own directory:
 * the plugin subprocess entry (`process.argv[1]`) always lives inside it.
 */
function pluginRequire(): NodeJS.Require {
  const entry = process.argv[1];
  const base = entry && isAbsolute(entry) ? dirname(entry) : process.cwd();
  return createRequire(join(base, "package.json"));
}

export async function loadPackage<T = Record<string, unknown>>(name: string): Promise<T> {
  const entry = pluginRequire().resolve(name);
  return (await import(pathToFileURL(entry).href)) as T;
}
