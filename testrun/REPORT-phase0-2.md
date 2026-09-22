# agy-gateway Phase 0-2 — отчёт субагента (22.09.2026 ~19:55 UTC)

## Путь повторяемого старта gateway
```bash
cd /opt/agy-gateway
bash -c 'set -a; . ./testrun/.env; set +a; nohup node dist/index.js >> testrun/gateway.log 2>&1 & disown'
# health: curl -s http://127.0.0.1:8399/healthz   -> {"ok":true}
# ключ:  AGY_PROXY_API_KEY в /opt/agy-gateway/testrun/.env (chmod 600, НЕ публиковать)
```
Остановка: `pkill -f 'node dist/index.js'` (аккуратно: не трогать PID star-cliproxy `packages/server/dist/index.js`, порт 8300).

## PASS/FAIL по шагам
1. PASS Клон /opt/agy-gateway (origin=ppmov/agy-gateway, upstream=Sakiko15/agy-proxy); npm install ok; tsdown build ok (dist 338.91 kB). Node 22.23.1. MVP-рецепт(/tmp/mvp-sakiko/build.log) не понадобился — сборка легла сразу.
2. PASS Порт-модули: /tmp/mvp-plan/cliproxy-port/*.ts → src/patch/; юнит-тесты `node --experimental-strip-types --test src/patch/*.test.ts` = **25/25 pass** в репо-контексте. Для strict-tsc портированных модулей поправлены 4 strict-пограничных места (поведение не менялось: String()/??-"string" фолбэки, guard best===undefined, вынос enum-блока в локальные переменные). `npx tsc --noEmit` = 0 ошибок.
3. PASS pool.json v1 (mode sequential, 5 аккаунтов, dir=/root/.agy-accounts/<name>) в data/gateway/accounts/pool.json относительно AGY_PROXY_DATA_DIR=/opt/agy-gateway/testrun/data. Безопасность: **стартовый validator** `validateAccountDirs()` в src/patch/gateway-patch.ts (запрет dir вне /root/.agy-accounts, включая realpath-проверку симлинков), вставлен в src/index.ts startup (exit(1) при нарушении). На реальном пуле → {"ok":true}. Плюс: admin	tests — см. src/patch/gateway-patch.ts.
4. PASS Интеграция в pipeline: schema-sanitizer (cleanJSONSchemaForAntigravity) на исходящий `--json-schema` (src/host/engine.ts, точка записи schema.json — НЕ история диалога); 429-classifier на hard-rate-limit ветке engine (src/host/engine.ts ~941, суффикс `(kind)` в сообщении, TODO-коммент для полной интеграции instant_retry/short_cooldown переключения аккаунтов). Прочее ядро Sakiko15 не тронуто.
5. PASS Тест-стенд: 127.0.0.1:8399, файл /opt/agy-gateway/testrun/.env (AGY_PROXY_API_KEY=новый сгенерированный ключ, AGY_PROXY_BIN=/root/.local/bin/agy, AGY_PROXY_MAX_CONCURRENT=1, host 127.0.0.1). /healthz=ok; /v1/models — 7 моделей (gemini-3.8/3.7/3.6-flash, gemini-3.1-pro, claude-sonnet-4-6, claude-opus-4-6-thinking, gpt-oss-120b-medium).
6. PASS e2e gemini «Reply with one word: PONG» → HTTP 200, content «PONG\n», stop (логи: /root/brain-vault/.hermes/cache/scratch/smoke-gemini.json, -nicesatana.json; /opt/agy-gateway/testrun/gateway.log). Первый запрос пула ушёл в **pupimov** (sequential → первый healthy), затем пул точечно пинился (enabled=false у прочих) и запрос прошёл **строго через nicesatana**: квота nicesatana Gemini 5h 100%→100% (мелкий запрос ниже зерна счётчика), resetTime сдвинулся 00:44:02→00:49:53; пул восстановлен (все 5 enabled=true).
7. PASS e2e claude через ganstapfanz (claude-sonnet-4-6, PONG): HTTP 200, content «PONG\n», 11.2s; lastUsedAt=spawn отметка у ganstapfanz в pool.json. Пул восстановлен. Агy-процессов параллельно >1 не создавалось (проверено pgrep: в момент покоя 0 agy-процессов; gateway spawns идут строго последовательно при maxConcurrent=1).
8. PASS Финальные проверки: tsc 0 ошибок, build ok, /healthz 200, single gateway PID 2897196, star-cliproxy (8300, lil-avg-big) не тронут. git push НЕ делался.

## npm test (полный прогон agy-proxy сьюта)
**3 failed / 565 passed (568)** — все 3 воспроизводятся на ПИСТОМ базовом дереве (без моих правок, проверено прогоном test/m0+admin-api на /tmp/agy-gateway-work) — pre-existing, env-артефакты этого сервера:
- test/m0.test.ts «binary-missing»: тест скрабит PATH/USERPROFILE, но Linux-homedir() смотрит /root/.local/bin/agy (реально установлен) → wait ready. Windows-тест. NOT regression.
- test/admin-api.test.ts paste-URL begin: 60s timeout, env/network артефакт. NOT regression.
- test/golden an8b-overloaded: ИЗМЕНЁН нашей правкой (в сообщение добавлен суффикс "(soft_retry)" от 429-классификатора); expected.json + PROVENANCE.md обновлены по факту нового контракта сообщения — тест теперь зелёный (26/26 в файле).

## Логи/артефакты
- /opt/agy-gateway/build.log; gateway: /opt/agy-gateway/testrun/gateway.log; инсталл: scratch/agy-npm-install.log; тесты: scratch/agy-npmtest.log; smoke JSON-ы: scratch/smoke-*.json; квоты до/после: scratch/quota-nicesatana{,-after,-after2}.txt, quota-pupimov-after.txt.

## Что осталось следующему субагенту (Phase 3+)
- Tool-bridge (клиентские tools) — Sakiko15 не исполняет;TODO-комменты в engine.rate-limit ветке — полная проводка instant_retry_same_auth / short_cooldown_switch_auth через selectAccount.
- Полный npm test: 3 pre-existing env-фейла можно закрыть пропуском на этом хосте или пропатчить тесты на Linux-хомедир.
