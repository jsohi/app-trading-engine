/*
 * ESLint 9 flat config for the Trading Engine web-ui.
 *
 * Strategy:
 *   - js.configs.recommended applies to all .js / .ts / .tsx.
 *   - typescript-eslint strict + stylistic type-checked configs are
 *     SCOPED to the TS files (.ts / .tsx). JavaScript files
 *     (local-rules/*.js, eslint.config.js itself) MUST NOT enable
 *     type-aware rules — those rules require parserOptions.project,
 *     and JS files are deliberately out of the TS project.
 *
 * Custom local rule:
 *   local/no-span-in-hot-path — forbids tracer.startSpan() inside
 *   onmessage/next handlers (allocation in streaming hot path).
 *
 * Lint fixtures:
 *   test/lint-fixtures/*.ts contain INTENTIONAL violations and are
 *   exercised by Vitest (`lint-fixtures.test.ts`) — those fixtures
 *   are excluded from `npm run lint` via the `ignores` block.
 */
import js from "@eslint/js";
import tseslint from "typescript-eslint";
import reactHooks from "eslint-plugin-react-hooks";
import importPlugin from "eslint-plugin-import";
import localRules from "./local-rules/index.js";

export default [
  {
    ignores: [
      "dist/**",
      "node_modules/**",
      "storybook-static/**",
      "playwright-report/**",
      "test-results/**",
      "coverage/**",
      // Build output of @trading/sbe-codecs (generated, lives outside
      // this workspace's src/ but resolvable via workspace symlink).
      "../sbe-typescript-generator/build/**",
      // INTENTIONAL violation files — exercised by lint-fixtures.test.ts
      // which spawns ESLint directly. `npm run lint` skips them.
      "test/lint-fixtures/**/*.ts",
    ],
  },
  // Baseline JS rules — apply to every file.
  js.configs.recommended,

  // -------- TypeScript files: type-aware strict + stylistic --------
  ...tseslint.configs.strictTypeChecked.map((cfg) => ({
    ...cfg,
    files: ["**/*.ts", "**/*.tsx"],
  })),
  ...tseslint.configs.stylisticTypeChecked.map((cfg) => ({
    ...cfg,
    files: ["**/*.ts", "**/*.tsx"],
  })),
  {
    files: ["**/*.ts", "**/*.tsx"],
    languageOptions: {
      ecmaVersion: 2022,
      sourceType: "module",
      globals: {
        // Browser globals used in src/.
        window: "readonly",
        document: "readonly",
        console: "readonly",
        WebSocket: "readonly",
        Worker: "readonly",
        URL: "readonly",
        URLSearchParams: "readonly",
        TextDecoder: "readonly",
        TextEncoder: "readonly",
        DataView: "readonly",
        ArrayBuffer: "readonly",
        Uint8Array: "readonly",
        BinaryType: "readonly",
        MessageEvent: "readonly",
        CloseEvent: "readonly",
        Event: "readonly",
        setTimeout: "readonly",
        clearTimeout: "readonly",
        setInterval: "readonly",
        clearInterval: "readonly",
        queueMicrotask: "readonly",
        performance: "readonly",
        crypto: "readonly",
        process: "readonly",
        self: "readonly",
      },
      parserOptions: {
        project: "./tsconfig.json",
        tsconfigRootDir: import.meta.dirname,
      },
    },
    plugins: {
      "react-hooks": reactHooks,
      import: importPlugin,
      local: localRules,
    },
    settings: {
      "import/resolver": {
        typescript: {
          alwaysTryTypes: true,
          project: "./tsconfig.json",
        },
        node: true,
      },
      react: {
        version: "19",
      },
    },
    rules: {
      // ---- Custom hot-path discipline (CLAUDE.md / saved feedback) ----
      "local/no-span-in-hot-path": "error",
      // APP-36 ESLint rule registrations (C1). Stubs return no diagnostics;
      // each is promoted from `off` → `error` in the commit that lands the
      // source they govern (no-bigint-to-number-coerce → C5; no-prototype
      // -pollution-from-decoder → C6; etc). Registering at C1 keeps the
      // rule namespace stable across the C1–C10 sequence.
      "local/no-bigint-to-number-coerce": "off",
      "local/no-banned-globals-in-worker": "off",
      "local/no-otel-attribute-outside-allowlist": "off",
      "local/no-prototype-pollution-from-decoder": "off",
      "local/no-dev-token-provider-outside-dev": "off",
      "local/no-crypto-with-storage-or-exfil": "off",
      "local/require-threading-allocation-tags": "off",

      // ---- bigint discipline ----
      "no-restricted-syntax": [
        "error",
        {
          selector: "CallExpression[callee.name='Number'] > Identifier",
          message:
            "Do not coerce values to Number — int64/uint64 fields are bigint. Use toFixed8() / nanosToDate() helpers in @trading/sbe-codecs.",
        },
        {
          selector: "CallExpression[callee.name='Number'] > Literal[bigint]",
          message:
            "Do not coerce a bigint literal to Number — precision loss above 2^53.",
        },
      ],

      // ---- Imports (TS analog of feedback_no_inline_fqcn.md) ----
      "import/no-namespace": "error",

      // ---- React hooks ----
      "react-hooks/rules-of-hooks": "error",
      "react-hooks/exhaustive-deps": "warn",

      // ---- TS strictness tuning ----
      "@typescript-eslint/no-unused-vars": [
        "error",
        {
          argsIgnorePattern: "^_",
          varsIgnorePattern: "^_",
          caughtErrorsIgnorePattern: "^_",
        },
      ],
      "@typescript-eslint/consistent-type-imports": [
        "error",
        { fixStyle: "inline-type-imports" },
      ],
      "@typescript-eslint/no-non-null-assertion": "warn",
      "@typescript-eslint/array-type": "off",
      "@typescript-eslint/no-inferrable-types": "warn",
    },
  },

  // Allow namespace imports for Node stdlib in build/config/scripts.
  {
    files: [
      "vite.config.ts",
      "vitest.config.ts",
      "playwright.config.ts",
      "scripts/**/*.{ts,mjs,js}",
      ".storybook/**/*.{ts,tsx}",
    ],
    rules: {
      "import/no-namespace": "off",
    },
  },

  // Test-only relaxations.
  {
    files: ["**/*.{test,spec}.{ts,tsx}", "test/**/*.{ts,tsx}", "e2e/**/*.{ts,tsx}"],
    rules: {
      "@typescript-eslint/no-non-null-assertion": "off",
      "@typescript-eslint/no-unsafe-assignment": "off",
      "@typescript-eslint/no-unsafe-call": "off",
      "@typescript-eslint/no-unsafe-member-access": "off",
      "@typescript-eslint/no-unsafe-argument": "off",
      "@typescript-eslint/no-unsafe-return": "off",
    },
  },

  // -------- Local ESLint plugin source (.js, no TS project) --------
  {
    files: ["local-rules/**/*.js"],
    languageOptions: {
      ecmaVersion: 2022,
      sourceType: "module",
      globals: {
        module: "readonly",
        require: "readonly",
        process: "readonly",
      },
    },
  },
];
