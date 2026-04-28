/**
 * ESLint rule: no-span-in-hot-path
 *
 * Forbids `tracer.startSpan(...)` (or any `*.startSpan(...)`) calls
 * inside hot-path message handlers. Hot-path handlers are functions/
 * methods named `onmessage`, `next`, or property keys with the same
 * names. `startSpan` allocates per call and would corrupt the
 * streaming hot path that APP-36 owns.
 *
 * Why a custom rule? `no-restricted-syntax` cannot do ancestor-aware
 * AST selectors reliably — an `onmessage` handler may host arbitrarily
 * deep call expressions, and we need to detect `startSpan` anywhere
 * inside that subtree.
 *
 * Span recording IS allowed on lifecycle events (worker start,
 * createStore.subscribe, error handlers) — those are not in this
 * rule's scope and are enforced by the OTel telemetry contract test.
 */

const HOT_HANDLER_NAMES = new Set(["onmessage", "next"]);

/**
 * Walks up the AST to find the nearest enclosing function-like
 * scope (FunctionDeclaration / FunctionExpression / ArrowFunctionExpression
 * / MethodDefinition value). Returns the AST node whose `key`/`id`
 * names that scope, or null if the function is anonymous and not
 * a property/method.
 *
 * @param {object} node ESLint AST node.
 * @returns {object|null} the enclosing named scope, or null.
 */
function nearestNamedScope(node) {
  let cur = node.parent;
  while (cur) {
    if (cur.type === "MethodDefinition" || cur.type === "Property") {
      return cur;
    }
    if (
      cur.type === "FunctionDeclaration" ||
      cur.type === "FunctionExpression" ||
      cur.type === "ArrowFunctionExpression"
    ) {
      const parent = cur.parent;
      if (parent && parent.type === "AssignmentExpression") {
        return parent;
      }
      if (parent && parent.type === "VariableDeclarator") {
        return parent;
      }
      if (parent && parent.type === "Property") {
        return parent;
      }
      if (parent && parent.type === "MethodDefinition") {
        return parent;
      }
      if (parent && parent.type === "PropertyDefinition") {
        return parent;
      }
    }
    cur = cur.parent;
  }
  return null;
}

/**
 * @param {object|null} scope
 * @returns {string|null}
 */
function scopeName(scope) {
  if (!scope) return null;
  if (
    scope.type === "MethodDefinition" ||
    scope.type === "Property" ||
    scope.type === "PropertyDefinition"
  ) {
    const k = scope.key;
    if (k && k.type === "Identifier") return k.name;
    if (k && k.type === "Literal" && typeof k.value === "string") return k.value;
    return null;
  }
  if (scope.type === "AssignmentExpression") {
    const lhs = scope.left;
    if (lhs && lhs.type === "MemberExpression" && lhs.property) {
      if (lhs.property.type === "Identifier") return lhs.property.name;
    }
    if (lhs && lhs.type === "Identifier") return lhs.name;
    return null;
  }
  if (scope.type === "VariableDeclarator") {
    if (scope.id && scope.id.type === "Identifier") return scope.id.name;
    return null;
  }
  return null;
}

/** @type {import('eslint').Rule.RuleModule} */
const rule = {
  meta: {
    type: "problem",
    docs: {
      description:
        "Forbid OpenTelemetry startSpan() calls inside hot-path onmessage/next handlers (per-message allocation).",
    },
    schema: [],
    messages: {
      hotPathSpan:
        "Do not call {{callee}}() inside a hot-path handler ({{handler}}). startSpan allocates per call. Record spans only on lifecycle events.",
    },
  },
  create(context) {
    return {
      CallExpression(node) {
        const callee = node.callee;
        if (
          callee.type !== "MemberExpression" ||
          callee.property.type !== "Identifier" ||
          callee.property.name !== "startSpan"
        ) {
          return;
        }
        const scope = nearestNamedScope(node);
        const name = scopeName(scope);
        if (name && HOT_HANDLER_NAMES.has(name)) {
          const sourceCode = context.sourceCode ?? context.getSourceCode();
          context.report({
            node,
            messageId: "hotPathSpan",
            data: {
              callee: sourceCode.getText(callee),
              handler: name,
            },
          });
        }
      },
    };
  },
};

export default rule;
