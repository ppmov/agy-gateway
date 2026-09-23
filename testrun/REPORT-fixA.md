# Fix-agent A — протокольные ошибки нового моста (overflow + whitespace-502)

Дата: 2026-09-22 ~22:20 UTC. Репо: /opt/agy-gateway (HEAD d283b62, ветка master, БЕЗ коммитов).
Скоуп соблюдён: изменены ТОЛЬКО src/patch/tool-bridge.ts (+.test.ts), src/server/openai-adapter.ts, test/openai-chat.test.ts.
Живых LLM-вызовов: 0 (все тесты офлайн на test/fake-agy.mjs / FAKE_AGY_EVENTS_FILE). engine.ts, star-cliproxy, gateway :8399 не тронуты мной (в рабочем дереве есть незакоммиченные правки engine.ts/gateway-patch.ts параллельного фикс-агента B — не мои).

## 1. Что изменено (фикс 1: переполнение контекста — мёртвый isAgyPromptOverflow стал живым)

Первопричина 5: agy сообщает о переполнении промпта через ТУ ЖЕ ветку фейлов, что и «no structured answer» (error-текст вместо JSON-envelope, либо `{error: …}`-объект без envelope-ключей — проверено: такой объект не парсится как envelope). Оба пути сходятся в `parseToolBridgeAnswer` — туда и встроена классификация. Успешные пути (tool_calls/текст) не затронуты: проверка стоит ТОЛЬКО на ветке «envelope === undefined».

- `ToolBridgeErrorCode` + `'prompt_overflow'`; `ToolBridgeError.status` = 413 для него (400 invalid_tools / 502 invalid_output — как было).
- `isAgyPromptOverflow` — прежний regex (PROMPT_OVERFLOW | prompt is too long | exceeds the maximum | Input is too long, /i), расширен реальными сигнатурами в тестах.
- НОВОЕ `agyPromptOverflowError(detail)`: message «prompt too long for model context: <текст agy>» (причина verbatim).
- НОВОЕ `agyOverflowCause(text)`: вынимает причину из error-канала (stderr-tail / result.error / raw-ответ), undefined на обычном мусоре.
- `parseToolBridgeAnswer`: на ветке no-envelope сначала overflow-проверка → 413-класс, иначе прежний invalid_output 502.
- `isRetryableToolBridgeError`: `prompt_overflow` НЕ ретраится (и по коду, и по overflow-тексту у code-less ошибок) — ретрай тем же промптом никогда не влезет; раньше сгорали 2 respawn-спавна.

### Честное ограничение (остаток 2 строки, вне моего скоупа)
E2E клиент видит сегодня HTTP 502 с кодом INVALID_OUTPUT, НО с явным message «prompt too long for model context: Input is too long: …» вместо прежнего сырого «no structured answer …». Почему не 413 по HTTP: recut в src/host/engine.ts (`recutToolBridgeSpan`, :336-361) ловит ToolBridgeError и жёстко проставляет `code: Err.INVALID_OUTPUT` — файл закреплён за параллельным агентом B. Проводка статуса — 2 строки в recut (сменить failure().code на err.code-маппинг; errors.ts + строка `prompt_overflow → 413`), рекомендую агенту B/следующему: классификация и тесты уже готовы (юниты ассертят code='prompt_overflow', status=413).

## 2. Что изменено (фикс 2: whitespace-content → 400 вместо 502)

`src/server/openai-adapter.ts` mapChatRequest: новый `rejectBlankText()` — пробельный/пустой контент = 400 invalid_request_error:
- user: «user message content must not be empty or whitespace-only…» (ИСКЛЮЧЕНИЕ: image-only turn с пустым текстом легитимен — движок сам добавляет промпт-просьбу к картинкам; проверено существующим тестом B-M5);
- system/developer: «system message content must not be empty…»;
- tool: «tool message content must not be empty or whitespace-only» (после курсор-правила agytc-/call_);
- assistant НЕ тронут: пустой текст при tool_calls — легитимная форма «только вызовы функций» (юнит-тест фиксирует).

Симптом Phase 4 (MCS robust-empty: «  » → 502 AGY_ERROR «request carries no user text») закрыт: теперь 400 на стороне маппера, до движка не доходит.

## 3. Тесты (все офлайн)

Новые:
- src/patch/tool-bridge.test.ts (+125 строк): TB-UNIT «prompt overflow classification» — сигнатуры regex, переклассификация текста и `{error}`-объекта, generic-502 не тронут, envelope-упоминание overflow в прозе НЕ флипается в 413, retry-цикл не спавнит повторов (asks=0), agyOverflowCause/agyPromptOverflowError юниты; TB-E2E — overflow-ответ fake-agy → явный message «prompt too long…» в HTTP-бади + РОВНО 1 спавн (0 ретраев), стриминг → SSE error frame с причиной.
- test/openai-chat.test.ts (+39 строк): whitespace user/system/developer/tool → 400 (statusCode проверен), легитимные формы (assistant+tool_calls, image-only user) остаются зелёными.

Выводы прогонов (факт):
- `npx tsc --noEmit` → 0 ошибок.
- `npx vitest run src/patch/tool-bridge.test.ts test/openai-chat.test.ts` → 90/90 pass.
- Полный `npx vitest run` в ЧИСТОМ env (env -i, без AGY_PROXY_*): **658 passed / 2 failed из 660** — оба фейла ровно известные pre-existing (admin-api paste-URL 60s-timeout, m0 binary-missing), не тронуты. База была 609/611 — критерий «не хуже 610+/611» перекрыт (мой прогон насчитал 660 тестов: в рабочем дереве также незакоммиченные тесты агента B; мои 2 файла — 90/90, до правок 76/76).

## 4. Diff-сумма (только мой скоуп)

| Файл | +/− |
|---|---|
| src/patch/tool-bridge.ts | +49 / −7 |
| src/patch/tool-bridge.test.ts | +125 / −0 |
| src/server/openai-adapter.ts | +28 / −3 |
| test/openai-chat.test.ts | +39 / −0 |
| ИТОГО | +241 / −10 (4 файла) |

(В полном `git diff --stat` видно ещё engine.ts/gateway-patch.ts — это незакоммиченная работа параллельного фикс-агента B, не моя; коммитить/пушить ничего не коммитил по условию.)

## 5. Рекомендации следующему
1. Проводка HTTP-статуса 413: recut engine.ts — в `failure()` использовать код из ToolBridgeError (маппинг `prompt_overflow → Err.PROMPT_OVERFLOW` или прямой status), errors.ts — строку в errorStatus/anthropicStatusFor. Юнит-тесты уже ассертят код/статус — после проводки снять маркер «502» с E2E-теста «overflow answer…» (заменить на 413).
2. Легаси-путь (запросы БЕЗ tools) на overflow тоже упирается в recut-классификацию — сейчас тот же сырой 502; проводка из п.1 закроет и его.
