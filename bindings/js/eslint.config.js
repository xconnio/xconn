import js from "@eslint/js";
import globals from "globals";
import tseslint from "typescript-eslint";

export default tseslint.config(
    { ignores: ["src/wasm/", "dist/", "node_modules/"] },
    js.configs.recommended,
    tseslint.configs.recommended,
    {
        languageOptions: {
            ecmaVersion: 2022,
            sourceType: "module",
            globals: { ...globals.browser, ...globals.node },
        },
        rules: {
            "@typescript-eslint/no-unused-vars": "off",
            "@typescript-eslint/no-explicit-any": "off",
        },
    },
);
