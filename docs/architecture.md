# Архитектура queryn-runtime

`queryn-runtime` — обязательный локальный backend Queryn. Desktop main
process использует версионированный JSON-RPC 2.0 поверх Unix socket или named
pipe. Headless CLI в основном вызывает те же runtime-сервисы напрямую, а
команда `serve` отдельно поднимает JSON-RPC boundary.

## Сервисы

- Project Service — явное открытие, проверка и миграция проекта.
- Extension Manager — staging, проверка, активация, подключение и rollback.
- Operation Registry и Policy Engine — схемы, permissions и risk-policy.
- Job Manager — прогресс, отмена, журнал и crash recovery.
- Runtime Supervisor — builtin/process/OCI/remote drivers.
- Artifact Ingestor — единственная точка проверки и публикации extension outbox
  в проект. Привилегированные built-ins используют атомарные core API и обязаны
  вернуть `publishedArtifactIds` в тот же job/session/artifact contract.
- Session Store — переносимая история запросов, сообщений, вызовов и подтверждений.
- Context Broker — компактный каталог, точечное исследование проекта и
  расширенный контекст с бюджетом и источниками.
- Connector Engine — возобновляемые project-scoped импорты.
- Model Manager — content-addressed cache и проверка зависимостей.
- Agent Orchestrator — граница провайдеров и поток пользовательского ответа,
  делегируемый единому `AgentKernel` tool-loop без доступа к shell.
- Diagnostics — проверка среды без требования AI или Docker.

`agent.chat` передаёт в `AgentKernel` историю сессии и схемы доступных операций.
Ядро принимает текст и вызовы операций, исполняет их через Job Manager,
получает observations и продолжает диалог до финального ответа. Рискованный
вызов может перевести job и chat run в `waiting-approval`, после чего
`agent.chat.approve` продолжает именно этот вызов. Отдельный AgentPlan,
pipeline и стадия предварительного планирования в текущем runtime отсутствуют.
Пользовательский ответ передаётся через `agent.output.delta`, а итог
сохраняется в переносимой истории сессии.

Для каждого обращения к модели Agent Kernel измеряет длительность и время до
первого текстового фрагмента. Итоговый `ChatRun` агрегирует число обращений,
входные и выходные токены, а для финальной генерации сохраняет TTFT и TPS.
Токены берутся из usage провайдера. Если совместимый провайдер не возвращает
usage, количество выходных токенов оценивается по тексту и явно помечается как
приблизительное. Метрики финального ответа записываются в `assistant-message`,
поэтому интерфейс может показать их без зависимости от runtime cache.

## Безопасная деградация

Отсутствие Docker, модели или расширения отражается диагностикой и состоянием capability. Оно не мешает открыть проект, читать Markdown, импортировать файлы и выполнять доступные builtin-операции.

Если встроенный Node не предоставляет SQLite FTS5, Context Indexer атомарно
создаёт удаляемый portable-индекс в `.queryn/index/context.json`. Это более
простой поиск, но проект и контекст не перестают работать.

## Жизненный цикл инструментов

- `job` запускает отдельный process/container для одного вызова;
- `project` переиспользует process внутри одного проекта до `runtime.stop` или idle timeout;
- `shared` переиспользует process между проектами и по умолчанию останавливает его после 300 секунд простоя.

`node-process` и `native-process` поддерживают все три режима. OCI намеренно
ограничен `job`: каждый вызов получает новые read-only input/models mounts и
свой outbox, не раскрывая контейнеру папку проекта или данные соседнего запуска.
Supervisor контролирует заявленный writable disk budget во время вызова и
повторно проверяет его перед принятием результата, поэтому oversized work/outbox
отклоняется до публикации.

## Версии расширений и локальное доверие

Установленные версии расширения хранятся side-by-side. `queryn.json` задаёт
переносимое требование (точная версия, `^`, `~`, `*` или `latest`), а
`.queryn/extensions/lock.json` фиксирует выбранную на этом компьютере версию и
integrity пакета. Registry выбирает Operation, Runtime, Context Provider,
Connector и Model Provider по lock конкретного проекта: обновление одного
проекта не переключает реализацию в другом.

Lock является удаляемым производным состоянием и пересобирается только из
установленных совместимых версий. Grants и сохранённые policy rules намеренно
не читаются из папки проекта: они лежат в локальном runtime state, привязанном к
абсолютному пути проекта. Перенос проекта сохраняет audit trail, но требует
заново подтвердить доверие на новом компьютере.

При старте и активации Extension Manager заново хеширует файлы установленной
версии по immutable install record. Изменённая или неполная версия не
регистрируется и показывается `diagnostics.doctor`; остальные проекты и built-in
tools продолжают работать.

MCP adapter отображает Tools в Operations, `resources/read` в Context Envelope,
а экспериментальный MCP task — во внутреннее ожидание Queryn Job. Отмена и
таймаут принадлежат Job Manager Queryn; MCP task не становится источником истины.
Регистрация MCP-сервера доступна через API runtime и тестовые сценарии, но
методы `mcp.server.*` пока не входят в dispatch публичного RPC. Desktop bridge
не должен считать эти методы рабочим end-to-end пользовательским путём до
добавления dispatch.

Model Manager пишет локальные project-to-digest usage records при reconcile
extension lock. `model.remove` сам вычисляет dependents и не доверяет переданному
клиентом списку; повреждённый usage record консервативно блокирует удаление до
диагностики.
