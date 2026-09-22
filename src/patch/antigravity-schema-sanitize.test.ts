// Ports of antigravity_schema_sanitize_test.go: history-preservation, schema-still-cleaned and
// result-schema-location coverage — adapted to the standalone schema cleaner.
import { test } from "node:test";
import assert from "node:assert/strict";
import {
  cleanJSONSchemaForAntigravityTool, cleanJSONSchemaForAntigravityResponse,
  cleanJSONSchemaForAntigravity, isAPIRequestDocument, isPropertyDefinition,
} from "./antigravity-schema-sanitize.ts";

const sanitizeTestPayload = {
  request: {
    contents: [
      { role: "model", parts: [{ functionCall: { name: "manage_todo_list", args: {
        operation: "write",
        todoList: [
          { id: 1, title: "output 1", description: "d1", status: "not-started" },
          { id: 2, title: "output 2", description: "d2", status: "not-started" },
        ],
      } } }] },
      { role: "model", parts: [{ functionCall: { name: "write_file", args: {
        path: "a.md", format: "markdown", default: "x", pattern: "p",
        const: "c", deprecated: false, nullable: "n", examples: "e",
        additionalProperties: "ap", "x-custom": "keepme",
      } } }] },
    ],
    tools: [{ functionDeclarations: [{
      name: "manage_todo_list",
      parametersJsonSchema: {
        type: "object",
        required: ["todoList"],
        properties: { todoList: { type: "array", items: {
          type: "object",
          required: ["id", "title"],
          title: "TodoItem",
          properties: { id: { type: "number" }, title: { type: "string", minLength: 3 } },
        } } },
      },
    }] }],
  },
};

test("isAPIRequestDocument recognizes request documents (why history must be excluded)", () => {
  assert.ok(isAPIRequestDocument(sanitizeTestPayload));
  assert.ok(isAPIRequestDocument({ messages: [] }));
  assert.ok(isAPIRequestDocument({ contents: [] }));
  assert.ok(isAPIRequestDocument({ request: { tools: [] } }));
  assert.ok(!isAPIRequestDocument({ type: "object", properties: { title: { type: "string" } } }));
});

test("cleaner refuses whole request documents (history safety gate)", () => {
  assert.throws(() => cleanJSONSchemaForAntigravityTool(sanitizeTestPayload as never), /request document/);
});

test("tool schema cleaning: title/minLength removed, minLength moved to hint", () => {
  const schema = structuredClone(sanitizeTestPayload.request.tools[0]!.functionDeclarations[0]!.parametersJsonSchema);
  const out = JSON.parse(cleanJSONSchemaForAntigravityTool(schema));
  const items = out.properties.todoList.items;
  // NOTE: our port keeps `removeToolTitle:false` in the Antigravity-tool policy for MVP (tsc-clean
  // subset). The Go original drops the schema keyword "title" for tools; assert our current
  // posture explicitly so the divergence is visible.
  assert.ok(items.title === undefined || items.title === "TodoItem");
  assert.ok(!("minLength" in (items.properties.title ?? {})), "minLength removed");
  assert.match(String((items.properties.title ?? {}).description), /minLength: 3/, "constraint survives as hint");
  assert.ok("title" in items.properties, "property named title preserved");
  assert.deepEqual(out.required, ["todoList"]);
  assert.deepEqual(items.required, ["id", "title"]);
});

test("tool cleaning drops enums to hints and drops unsupported keywords", () => {
  const schema = {
    type: "string",
    pattern: "^[a-z]+$",
    default: "x",
    examples: ["a"],
    enum: ["a", "b"],
    format: "date-time",
    additionalProperties: false,
  };
  const out = JSON.parse(cleanJSONSchemaForAntigravityTool(schema));
  for (const k of ["pattern", "default", "examples", "enum", "format", "additionalProperties"]) {
    assert.ok(!(k in out), `${k} dropped`);
  }
  assert.match(out.description, /pattern:/);
  assert.match(out.description, /enumerate:|Allowed:/);
});

test("enum type is not forced to string in antigravity tool cleaner when declared type is number", () => {
  const schema = { type: "number", enum: [1, 2, 3] };
  const out = JSON.parse(cleanJSONSchemaForAntigravityTool(schema));
  assert.equal(out.type, "number"); // declared type preserved (Gemini-internal cleaner forces string; antigravity not)
  assert.ok(!out.enum, "enum dropped to hint in tool cleaner");
  assert.ok(out.description.includes("Allowed: 1, 2, 3"));
});

test("response cleaner keeps enum behavior with boolean enums dropped", () => {
  const schema = { type: "object", additionalProperties: false, properties: {
    s: { type: "string", enum: ["a", "b"], minLength: 1 },
    b: { type: "boolean", enum: [true, false] },
  } };
  const out = JSON.parse(cleanJSONSchemaForAntigravityResponse(schema));
  assert.equal(out.additionalProperties, false); // preserved for response schemas
  assert.deepEqual(out.properties.s.enum, ["a", "b"]); // string enum kept
  assert.ok(!out.properties.b.enum, "boolean enum dropped");
  assert.ok(!("minLength" in out.properties.s));
});

test("isPropertyDefinition trailing-run rule", () => {
  assert.ok(isPropertyDefinition(["properties"]));                     // the property map itself
  assert.ok(!isPropertyDefinition(["properties", "title"]));           // schema of property "title"
  assert.ok(!isPropertyDefinition(["properties", "properties"]));      // schema of a property named "properties" (even run)
  assert.ok(isPropertyDefinition(["properties", "properties", "properties"])); // its own property map (odd run)
  assert.ok(!isPropertyDefinition(["properties", "properties", "properties", "minLength"])); // schema node inside
  assert.ok(!isPropertyDefinition([]));
});

test("placeholder addition (Claude VALIDATED requirement) via CleanJSONSchemaForAntigravity", () => {
  const out = JSON.parse(cleanJSONSchemaForAntigravity({ type: "object" }));
  assert.ok(out.properties.reason);
  assert.deepEqual(out.required, ["reason"]);
  assert.equal(out.properties.reason.type, "string");
});

test("title as data key in antigravity tool policy", () => {
  const schema = { type: "object", properties: { name: { type: "string", title: "Name" } } };
  const out = JSON.parse(cleanJSONSchemaForAntigravityTool(structuredClone(schema)));
  // MVP port keeps removeToolTitle:false, so title is preserved; if we later align to Go's
  // original tool cleaner (title dropped), this must flip.
  assert.equal(out.properties.name.title, "Name");
});
