import { readFile, writeFile, readdir } from "node:fs/promises";
import { createHash } from "node:crypto";
import path from "node:path";
import { brotliCompressSync, gzipSync, constants } from "node:zlib";
const output = path.resolve(process.argv[2] ?? "target/browser-wasm/pkg");

async function javascript(directory, prefix = "") {
  const files = [];
  for (const entry of await readdir(path.join(directory, prefix), {
    withFileTypes: true,
  })) {
    const name = path.join(prefix, entry.name);
    if (entry.isDirectory()) files.push(...(await javascript(directory, name)));
    else if (name.endsWith(".js")) files.push(name);
  }
  return files.sort();
}

const results = [];
for (const variant of ["json", "codecs", "full"]) {
  const directory = path.join(output, variant);
  const sizes = {
    wasm: { raw: 0, gzip: 0, brotli: 0 },
    javascript: { raw: 0, gzip: 0, brotli: 0 },
  };
  const sha256 = {};
  const files = [
    "wasm/xconn_browser_bg.wasm",
    ...(await javascript(directory)),
  ];
  for (const file of files) {
    const bytes = await readFile(path.join(directory, file));
    const gzip = gzipSync(bytes, { level: 9 });
    const brotli = brotliCompressSync(bytes, {
      params: { [constants.BROTLI_PARAM_QUALITY]: 11 },
    });
    const group = file.endsWith(".wasm") ? sizes.wasm : sizes.javascript;
    group.raw += bytes.length;
    group.gzip += gzip.length;
    group.brotli += brotli.length;
    sha256[file] = createHash("sha256").update(bytes).digest("hex");
    await writeFile(path.join(directory, `${file}.gz`), gzip);
    await writeFile(path.join(directory, `${file}.br`), brotli);
  }
  sizes.total = Object.fromEntries(
    ["raw", "gzip", "brotli"].map((key) => [
      key,
      sizes.wasm[key] + sizes.javascript[key],
    ]),
  );
  results.push({ variant, ...sizes, sha256 });
}
const report = {
  scope:
    "Actual bindings/js package: WASM, generated glue and all compiled browser client modules",
  toolchain: (await readFile(path.join(output, "full/toolchain.txt"), "utf8"))
    .trim()
    .split("\n"),
  compression: {
    gzipLevel: 9,
    brotliQuality: 11,
    totals:
      "Sum of independently compressed runtime files; unbundled and unminified",
  },
  results,
};
await writeFile(
  path.join(output, "sizes.json"),
  JSON.stringify(report, null, 2) + "\n",
);
console.table(
  results.map(({ variant, wasm, javascript, total }) => ({
    variant,
    "WASM KiB": (wasm.raw / 1024).toFixed(1),
    "JS KiB": (javascript.raw / 1024).toFixed(1),
    "total gzip KiB": (total.gzip / 1024).toFixed(1),
    "total Brotli KiB": (total.brotli / 1024).toFixed(1),
  })),
);
