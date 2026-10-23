import { readFile, writeFile } from "node:fs/promises";
import path from "node:path";
const root = path.resolve(import.meta.dirname, "../../..");
const rust = path.join(root, "crates/wampproto/src");
const schema = await readFile(path.join(rust, "messages/schema.rs"), "utf8");
const uris = await readFile(path.join(rust, "uris.rs"), "utf8");
const codes = [...schema.matchAll(/(\d+)\s*=>\s*layout!\(\s*code,\s*"([A-Z_]+)"/g)];
if (!codes.length) throw new Error("No Rust message codes found");
let text = "// Generated from the Rust WAMP schema and URIs. Do not edit.\n";
text += `export const Code = {\n${codes.map(([, code, name]) => `    ${name}: ${code},`).join("\n")}\n} as const;\n`;
text += `export const URI = {\n${[...uris.matchAll(/pub const (\w+): &str = "([^"]+)";/g)]
    .map(([, name, value]) => `    ${name}: ${JSON.stringify(value)},`)
    .join("\n")}\n} as const;\n`;
await writeFile(path.join(root, "bindings/js/src/constants.ts"), text);
