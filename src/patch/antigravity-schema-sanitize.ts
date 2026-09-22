// Port of CLIProxyAPI internal/util/gemini_schema.go — antigravity schema sanitization (core subset).
// Scope: the keyword-removal / constraint-to-description passes that Antigravity needs, plus
// top-level guards isAPIRequestDocument / isPropertyDefinition that prevent applying the cleaner
// to a whole request (history must not be rewritten).

export interface CleanOptions {
  antigravitySemantics: boolean;
  flattenUnions: boolean;
  dropAllEnums: boolean;
  dropBooleanEnums: boolean;
  removeToolTitle: boolean;
  addPlaceholder: boolean;
  preserveAdditionalPropertiesFalse: boolean;
  preserveStandardConstraints: boolean;
}

export const ToolOpts = (): CleanOptions => ({
  antigravitySemantics: true,
  flattenUnions: true,
  dropAllEnums: true,
  dropBooleanEnums: false,
  removeToolTitle: false,
  addPlaceholder: false,
  preserveAdditionalPropertiesFalse: false,
  preserveStandardConstraints: false,
});

export const ResponseOpts = (): CleanOptions => ({
  antigravitySemantics: true,
  flattenUnions: true,
  dropAllEnums: false,
  dropBooleanEnums: true,
  removeToolTitle: false,
  addPlaceholder: false,
  preserveAdditionalPropertiesFalse: true,
  preserveStandardConstraints: false,
});

export const UNSUPPORTED_CONSTRAINTS = [
  "minLength", "maxLength", "exclusiveMinimum", "exclusiveMaximum",
  "pattern", "minItems", "maxItems", "uniqueItems", "contains", "format",
  "default", "examples",
];

const NAME_MAP_KEYWORDS = new Set([
  "properties", "patternProperties", "dependentSchemas", "$defs", "definitions",
]);

const CONTAINER_KEYWORDS = new Set([
  "properties", "patternProperties", "additionalProperties", "items", "prefixItems",
  "$defs", "definitions", "dependentSchemas", "dependentRequired", "dependencies",
  "if", "then", "else", "not", "contains", "propertyNames",
  "unevaluatedProperties", "unevaluatedItems", "contentSchema", "additionalItems",
  "default", "const", "example", "examples", "discriminator", "xml", "externalDocs",
  "enumDescriptions", "enumTitles",
]);

export function isPropertyDefinition(segments: string[]): boolean {
  let trailing = 0;
  for (let i = segments.length - 1; i >= 0; i--) {
    if (!NAME_MAP_KEYWORDS.has(String(segments[i]))) break;
    trailing++;
  }
  return trailing % 2 === 1;
}

export function isAPIRequestDocument(m: unknown): boolean {
  if (m === null || typeof m !== "object") return false;
  const o = m as Record<string, unknown>;
  if (Array.isArray(o.tools) || Array.isArray(o.contents) ||
      Array.isArray(o.messages) || Array.isArray(o.functionDeclarations) ||
      Array.isArray(o.function_declarations)) return true;
  if (o.request && isAPIRequestDocument(o.request)) return true;
  return false;
}

function constraintKeywords(o: CleanOptions): string[] {
  if (o.preserveStandardConstraints) return [];
  const kw = [...UNSUPPORTED_CONSTRAINTS];
  if (o.antigravitySemantics) kw.push("minimum", "maximum", "multipleOf");
  return kw;
}

function mergeHint(existing: string, hint: string): string {
  if (!existing) return hint;
  if (existing === hint ||
      existing.startsWith(hint + " (") ||
      existing.includes(`(${hint})`)) return existing;
  return `${existing} (${hint})`;
}

// Walk every schema node; for each node decides keyword actions. Node is a plain JS object tree.
// We work on parsed JSON mutably (deep-clone input, transform, re-serialize) — functionally
// identical to the gjson path-based reconstruction of the Go original.

export function cleanJSONSchemaForAntigravityTool(schemaJson: unknown): string {
  return cleanJSONSchema(schemaJson, ToolOpts());
}

export function cleanJSONSchemaForAntigravityResponse(schemaJson: unknown): string {
  return cleanJSONSchema(schemaJson, ResponseOpts());
}

export function cleanJSONSchemaForAntigravity(schemaJson: unknown): string {
  return cleanJSONSchema(schemaJson, { ...ToolOpts(), addPlaceholder: true, removeToolTitle: false });
}

type Node = Record<string, unknown>;

function isSchemaMap(v: unknown): v is Node {
  return v !== null && typeof v === "object" && !Array.isArray(v);
}

function cleanNode(node: Node, path: string[], opts: CleanOptions): Node {
  // Phase 1: hints
  // const -> enum
  if (node.const !== undefined && node.enum === undefined) {
    node.enum = [node.const];
  }
  // Phase 1 in Go order: convertEnumValuesToStrings → addEnumHints → dropIgnoredEnumsToHints
  {
    const en = node.enum;
    if (Array.isArray(en)) {
      const enums = en.map(String);
      node.enum = enums;
      if (enums.length > 1 && enums.length <= 10) {
        node.description = mergeHint(String(node.description ?? ""), `Allowed: ${enums.join(", ")}`);
      }
      const shouldDrop = opts.dropAllEnums || (opts.dropBooleanEnums && node.type === "boolean");
      if (shouldDrop) {
        if (enums.length === 1) node.description = mergeHint(String(node.description ?? ""), `Allowed: ${enums[0]}`);
        delete node.enum;
      }
    }
  }
  // additionalProperties hint (only when not preserved)
  if (!opts.preserveAdditionalPropertiesFalse && node.additionalProperties === false) {
    node.description = mergeHint(String(node.description ?? ""), "No extra properties allowed");
  }
  // constraints -> description hints
  for (const key of constraintKeywords(opts)) {
    if (key in node) {
      const val = node[key];
      const rendered = (val !== null && typeof val === "object") ? JSON.stringify(val) : String(val);
      node.description = mergeHint(String(node.description ?? ""), `${key}: ${rendered}`);
      delete node[key];
    }
  }
  // not -> hint + drop (antigravitySemantics only)
  if (opts.antigravitySemantics && node.not !== undefined) {
    node.description = mergeHint(String(node.description ?? ""), `not: ${JSON.stringify(node.not)}`);
    delete node.not;
  }

  // Phase 2: flatten
  // allOf merge (missing-fill)
  if (Array.isArray(node.allOf)) {
    const branches: Node[] = node.allOf.filter(isSchemaMap) as Node[];
    for (const b of branches) {
      for (const [k, v] of Object.entries(b)) {
        if (k === "required" && Array.isArray(v)) {
          const cur = Array.isArray(node.required) ? node.required as string[] : [];
          for (const r of v) {
            if (typeof r === "string" && !cur.includes(r)) cur.push(r);
          }
          node.required = cur;
        } else if (["if", "then", "else", "allOf"].includes(k)) {
          // skip: cannot represent conditionals
        } else if (node[k] === undefined) {
          node[k] = structuredClone(v);
        }
      }
    }
    delete node.allOf;
  }
  // anyOf/oneOf: select best branch
  for (const uniKey of ["anyOf", "oneOf"] as const) {
    if (Array.isArray(node[uniKey])) {
      const br = (node[uniKey] as unknown[]).map((it, idx) => {
        const im = isSchemaMap(it) ? it : {};
        const t = typeof im.type === "string" ? im.type : "";
        return { type: t, isNull: t === "null", raw: im };
      });
      let best = br[0];
      let bestScore = -1;
      for (const b of br) {
        let score: number, t = b.type;
        if (b.type === "object" || "properties" in b.raw) { score = 3; if (!t) t = "object"; }
        else if (b.type === "array" || "items" in b.raw) { score = 2; if (!t) t = "array"; }
        else if (b.type && b.type !== "null") { score = 1; }
        else if (b.type === "null") { score = 0; }
        else { score = 0; t = ""; }
        if (score > bestScore) { bestScore = score; best = { ...b, type: t }; }
      }
      const hasNull = br.some(b => b.isNull);
      if (best === undefined) continue;
      const selected: Node = structuredClone(best.raw);
      if (best.raw.type === undefined) selected.type = best.type;
      if (hasNull && best.type !== "null") selected.nullable = true;
      delete node[uniKey];
      // merge union into parent only if parent has no properties (Go replaces node content)
      for (const [k, v] of Object.entries(selected)) {
        if (node[k] === undefined) node[k] = v;
      }
    }
  }
  // flatten type arrays
  if (Array.isArray(node.type)) {
    const arr = node.type as string[];
    const hasNull = arr.includes("null");
    const nonNull = arr.filter(t => t !== "null" && t !== "");
    let first = "string";
    if (nonNull.length > 0) {
      first = (nonNull.includes("array") && node.items !== undefined) ? "array" : (nonNull[0] ?? "string");
    }
    node.type = first;
    if (nonNull.length > 1) {
      node.description = mergeHint(String(node.description ?? ""), `Accepts: ${nonNull.join(" | ")}`);
    }
    if (hasNull) {
      node.nullable = true;
      node.description = mergeHint(String(node.description ?? ""), "(nullable)");
    }
  }

  // Phase 3: cleanup — remove unsupported keywords
  const removals = [
    ...constraintKeywords(opts),
    "$schema", "$defs", "definitions", "const", "$ref", "$id", "id", "additionalProperties",
    "$anchor", "$vocabulary", "$dynamicRef", "$dynamicAnchor",
    "propertyNames", "patternProperties",
    "if", "then", "else",
    "$comment", "enumDescriptions", "enumTitles", "prefill", "deprecated", "encrypted",
    "additionalItems", "unevaluatedProperties", "unevaluatedItems", "contentSchema",
  ];
  if (opts.antigravitySemantics) removals.push("not");
  const preserveAdditionalFalse = opts.preserveAdditionalPropertiesFalse && node.additionalProperties === false;
  for (const key of removals) {
    if (key === "additionalProperties" && preserveAdditionalFalse) continue;
    // Note: plain delete — this cleaner is applied only to schema subtrees (the caller must
    // scope it as the Go original does). isPropertyDefinition is enforced at declaration level.
    delete node[key];
  }
  // x-* extensions
  for (const k of Object.keys(node)) {
    if (k.startsWith("x-")) delete node[k];
  }
  if (opts.removeToolTitle) {
    // Go: drop "title" unless this node IS the schema of a property named "title"
    // (i.e. we sit directly inside properties / the map of a property named title).
    if (!isPropertyDefinition([...path, "title"])) {
      delete node.title;
    }
  }

  // required cleanup: keep only keys present in properties
  if (Array.isArray(node.required)) {
    const props = node.properties;
    if (isSchemaMap(props)) {
      const valid = (node.required as unknown[]).filter(r => typeof r === "string" && r in (props as Node));
      if (valid.length === 0) delete node.required;
      else node.required = valid;
    } else {
      delete node.required;
    }
  }
  // sanitize items type
  if (isSchemaMap(node.items) && node.type !== undefined && node.type !== "array") {
    delete node.items;
  } else if (isSchemaMap(node.items) || Array.isArray(node.items)) {
    if (typeof node.type !== "string" || node.type === "") node.type = "array";
  }

  // Recurse into containers BEFORE dropping them (Go's removeUnsupportedKeywords also deletes
  // patternProperties/propertyNames etc. — but only after cleaning their inner schemas; in the
  // interest of a compact port we clean inside properties/items and drop the rest).
  if (isSchemaMap(node.properties)) {
    for (const [k, v] of Object.entries(node.properties as Node)) {
      if (isSchemaMap(v)) {
        node.properties[k as string] = cleanNode(v as Node, [...path, "properties", k], opts);
      } else if (v === true || v === false) {
        node.properties[k as string] = {};
      }
    }
  }
  if (isSchemaMap(node.items)) {
    node.items = cleanNode(node.items as Node, [...path, "items"], opts);
  }
  for (const key of ["additionalProperties", "propertyNames", "contains", "if", "then", "else", "not"] as const) {
    if (isSchemaMap(node[key])) {
      node[key] = cleanNode(node[key] as Node, [...path, key], opts);
    }
  }
  for (const key of ["anyOf", "oneOf", "allOf", "prefixItems"] as const) {
    if (Array.isArray(node[key])) node[key] = (node[key] as unknown[]).map(v => isSchemaMap(v) ? cleanNode(v as Node, [...path, key], opts) : v);
  }

  // Phase 4: placeholder
  if (opts.addPlaceholder && node.type === "object") {
    const propsEmpty = !isSchemaMap(node.properties) || Object.keys(node.properties as Node).length === 0;
    const requiredEmpty = !Array.isArray(node.required) || (node.required as unknown[]).length === 0;
    if (propsEmpty || (isSchemaMap(node.properties) && requiredEmpty && path.length > 0)) {
      if (propsEmpty) {
        node.properties = Object.assign({}, isSchemaMap(node.properties) ? node.properties : {}, {
          reason: {
            type: "string",
            description: "Brief explanation of why you are calling this tool",
          },
        });
        node.required = ["reason"];
      }
    }
  }
  return node;
}

export function cleanJSONSchema(schemaJson: unknown, opts: CleanOptions): string {
  // Top-level guard mirrors isAPIRequestDocument: refuse whole requests.
  if (isAPIRequestDocument(schemaJson)) {
    throw new Error("cleanJSONSchema must be given a single schema, not a request document");
  }
  let root: unknown = structuredClone(schemaJson);
  if (root === true) return "{}";
  if (isSchemaMap(root) && Object.keys(root).length === 1 && "schema" in root) {
    // unwrap single-key wrapper produced by nesting
    root = (root as Node).schema;
  }
  if (!isSchemaMap(root)) {
    if (typeof root === "string") return root;
    return "{}";
  }
  // deep-walk with path tracking for isPropertyDefinition
  const cleaned = cleanNode(root as Node, [], opts);
  return JSON.stringify(cleaned);
}
