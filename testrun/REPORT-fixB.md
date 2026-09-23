# Отчёт fix-агент B — engine agy-gateway: большие промпты + 429-ротация + покрытие тестов

Дата: 22.09.2026 (UTC). Репо: /opt/agy-gateway @ d283b62 (рабочее дерево, НЕ коммитил — по запрету).
Живых LLM-вызовов: 0 (все прогоны на fake-agy). tool-bridge.ts / openai-adapter.ts / star-cliproxy / gateway :8399 — не тронуты.

## Критерии приёмки

| Критерий | Факт |
|---|---|
| `npx tsc --noEmit` = 0 ошибок | ✅ 0 ошибок |
| vitest полный в чистом env ≥ 610+/611 | ✅ **644 passed / 2 failed из 646** — 2 фейла ровно pre-existing из базы (admin-api paste-URL 60s-timeout, m0 binary-missing — не чинил по ТЗ). База 609/611 + 35 новых тестов зелёные |
| Порт-сьюты node --test (429-classifier + schema-sanitize) | ✅ 25/25 |
| Отчёт testrun/REPORT-fixB.md | ✅ этот файл |

Прогон полного vitest: `env -u AGY_PROXY_* npx vitest run` (чистое окружение — ловушка «грязного» env из аудита п.6 учтена).

## 1. Первопричина 6 (E2BIG): большие промпты без tools → stream-json/stdin

**Файл:** src/host/engine.ts.

- Новая экспортированная константа `STDIN_PROMPT_THRESHOLD = 100 * 1024` (безопасный запас под MAX_ARG_STRLEN ~128KB на один argv-элемент).
- В сборке спавна: не-bridge запрос с промптом ≥ порога получает `else if (prompt.length >= STDIN_PROMPT_THRESHOLD)` ветку рядом с bridge-переписыванием argv — argv очищается от пары `-p <prompt>`, добавляется `--input-format stream-json`, промпт уходит одной NDJSON-строкой `{event:'user', message:{role:'user', content}}` в stdin (stdinData → runner пишет и закрывает EOF — тот же транспорт, что у tool-bridge, проверен живьём против agy 1.6.5).
- Поведение ответов сохранено: без envelope-схемы, без `--sandbox`, флаги model/effort/conversation/add-dir/extraArgs не тронуты; промпты < 100KB идут по прежнему `-p`-пути (ни одного изменения для существующих клиентов).
- **Тест:** test/engine-stdin-transport.test.ts (4 теста, fake-agy): промпт ~110KB — в argv НЕТ промпта (ни целиком, ни хвостом >1KB), есть `--input-format stream-json`, ран завершается успешно; короткий промпт держит легаси `-p`; jsonSchema-файл-ветка не задета.

## 2. Первопричина 3 (тупые ретраи): полная интеграция 429-классификатора

**Файлы:** src/patch/gateway-patch.ts (переписан decideEngineCooldown), src/host/engine.ts (settle-гейт + selectAccountFor).

- **Ключевая находка:** прежний `decideEngineCooldown` классифицировал свободный текст agy («429 RESOURCE_EXHAUSTED … Resets in 21m25s») как soft_retry и увёл бы реальные hard-429 в ShortCooldownStore вместо recordFailure. Убедился прогоном на реальном тексте fake-agy — первый вариант интеграции дал бы именно этот регресс.
- **Решение — гибридная маршрутизация окна:** blunt-гейт `looksLikeHardRateLimit` остаётся решателем «ЭТО вообще серверный 429» (его узкие сигнатуры — защита от ghost-cooldown, см. rate-limit-classify.test.ts), а `decideEngineCooldown` решает «КАК реагировать»: окно ретрая берётся из JSON-путей классификатора (RetryInfo.retryDelay / quotaResetDelay / «after Ns»), а для свободного текста — из парсера пула («Resets in 21m25s» / «retry after 30 seconds»). Итог:
  - окно <3s → `instant_retry`: БЕЗ какого-либо кулдауна (тот же аккаунт сразу готов);
  - окно <5m → `short_cooldown`: окно в ShortCooldownStore (ключ account|family, самочистящееся) + смена аккаунта;
  - окно ≥5m или quota_exhausted без resetTime → `full_cooldown`: `pool.recordFailure` (учитывает реальное окно сброса из текста, +10s буфер);
  - hard-сигнатура без распознаваемого окна → `full_cooldown` (консервативно = прежнее поведение);
  - JSON 429 без delay (классификатор soft_retry) → `instant_retry`.
- **Engine-сторона:** `applyRateLimitCooldown()` заменил прямой `recordFailure` в settle-гейте; `selectAccountFor()` — новая точка выбора аккаунта (первичный выбор, autoFallbackModel, retry-перевыбор): подмешивает short-cooldown окна пула как «занят» и перевыбирает — <5m-окна переключают аккаунт, НЕ замораживая его на тупые 15m.
- **Контракт сообщения сохранён 1:1:** суффикс `(...)` в «Google Antigravity quota / rate limit reached (...)» по-прежнему ставит classifyAgy429 — **золотой an8b-overloaded НЕ изменился** (там «model overloaded» → soft_retry, суффикс прежний; проверено полным прогоном golden).
- **Тесты:** test/gateway-patch.test.ts (8 кейсов маршрутизации) + test/engine-429-integration.test.ts (5 e2e на fake-agy: длинное окно → cooldown+ротация на аккаунт B и причина «Resets in 21m25s» в пуле; short-окно НЕ даёт 15m-пул-кулдауна; soft overload не трогает пул; long-prompt через тот же пул-путь).

## 3. Тестовые дыры аудита (находки 2,3,4)

- **validateAccountDirs** (gateway-patch.ts:45-79): 8 кейсов в test/gateway-patch.test.ts — пустой пул, legacy-записи без dir, in-pool реальные/несуществующие каталоги, отказ dir вне корня, отказ `..`-traversal, отказ symlink-эскейпа (realpath, «resolves to»), отказ самого корня (rm -rf всего пула невозможен), отказ первой offending-записи, дефолтный ACCOUNTS_ROOT=/root/.agy-accounts.
- **Quota-skip ≤2%** (pool.ts:525-533): 9 кейсов в test/pool-quota-skip.test.ts — 5h ≤2% с будущим resetTime → пропуск; >2% → доступен; weekly ≤1% → пропуск; прошлый resetTime → возврат в ротацию; ≤2% без resetTime → остаётся (консервативно); битый resetTime → не подавляет (NaN-защита); family-scoped (google-квота не трогает anthropic); весь пул по квоте → null; ровно-на-пороге → пропуск (≤). Квота вводится через `updateAccountQuotas` — продакшн-шов квота-поллера (эквивалент mock quota-service из плана фазы 1).

## 4. Изменённые файлы

| Файл | Что |
|---|---|
| src/host/engine.ts | +STDIN_PROMPT_THRESHOLD, stdin-транспорт длинных не-bridge промптов, applyRateLimitCooldown, selectAccountFor, ShortCooldownStore, переписан 429-TODO |
| src/patch/gateway-patch.ts | decideEngineCooldown переписан (instant_retry/short_cooldown/full_cooldown + окно из свободного текста), +parseResetDurationMs |
| test/gateway-patch.test.ts | НОВЫЙ: 16 тестов (dir-guard 8 + 429-роутер 8) |
| test/pool-quota-skip.test.ts | НОВЫЙ: 9 тестов quota-skip |
| test/engine-stdin-transport.test.ts | НОВЫЙ: 4 теста E2BIG |
| test/engine-429-integration.test.ts | НОВЫЙ: 5 e2e тестов интеграции |
| Золотой an8b-overloaded | НЕ менялся — контракт сообщения не задет (проверено прогоном) |

Вне скоупа не заходил: tool-bridge.ts, openai-adapter.ts, admin-api, runner, pool.ts (сам код пула не менялся — только тесты). Не коммитил, не пушил. Секретов в отчёте нет.
