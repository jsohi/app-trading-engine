/**
 * lint-staged config — must run from a directory that contains the
 * ESLint flat config. Our eslint.config.js lives in `web-ui/`, so
 * staged TS/TSX files under web-ui/ get linted from inside web-ui/
 * with paths re-rooted to that directory.
 *
 * Prettier sees `.prettierrc` in either the root or web-ui (via its
 * own discovery), so the prettier task can run from the root with
 * absolute paths. (Prettier treats absolute paths fine.)
 */
import { relative } from "node:path";
import process from "node:process";

const repoRoot = process.cwd();
const webUiAbs = `${repoRoot}/web-ui`;

/**
 * @param {string[]} files absolute paths supplied by lint-staged
 * @returns {string[]} commands to execute
 */
function eslintWebUi(files) {
  const inWebUi = files
    .map((f) => relative(webUiAbs, f))
    .filter((p) => p.length > 0 && !p.startsWith(".."));
  if (inWebUi.length === 0) {
    return [];
  }
  const args = inWebUi.map((p) => `'${p}'`).join(" ");
  return [`bash -c "cd web-ui && eslint --fix --max-warnings 0 ${args}"`];
}

export default {
  "web-ui/**/*.{ts,tsx}": eslintWebUi,
  "*.{ts,tsx,json,md,yml,yaml}": ["prettier --write"],
};
