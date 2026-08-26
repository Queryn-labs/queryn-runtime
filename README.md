# osnova-runtime

Локальный control plane Osnova Reborn. Он управляет проектами, расширениями, операциями, заданиями, контекстом, моделями и диалоговым агентом с инструментами. AI и OCI являются опциональными возможностями: проект открывается и остаётся полезным без них.
Каноническая [страница документации](https://github.com/Queryn-labs/osnova-docs) описывает архитектурную роль runtime и его контракты.

## Статус

Обязательный локальный control plane версии `0.2.0`. Runtime предоставляет
host-среду расширений, локальный RPC, операции, jobs, контекст, модели и
агентный цикл. AI и OCI остаются опциональными возможностями.

## Stack

- TypeScript и Node.js
- локальный JSON-RPC v1
- pnpm 10.5.2

## Команды

```bash
npm install --ignore-scripts
npm run build
npm test
node dist/cli.js selftest
node dist/cli.js serve
node dist/cli.js help
```

В отдельном клоне `npm run pretest` ожидает соседний репозиторий
`../osnova-spec` со скриптами проверки контрактов и гигиены комментариев. Для
локального запуска клонируйте `osnova-spec` рядом с runtime или используйте
layout из CI.

## Границы

- Runtime не владеет пользовательскими данными: долговечное состояние хранится в открытой папке проекта.
- Extension-инструменты не получают прямой доступ на запись к проекту. Результаты
  проходят через outbox и `ArtifactIngestor`. Привилегированные built-ins пишут
  только через атомарные API `osnova-core` и возвращают artifact ids в тот же job.
- Агент вызывает только зарегистрированные операции и не получает shell/filesystem API.
- Локальный RPC использует случайный адрес и bearer-токен экземпляра.

Запуск `serve` печатает JSON с адресом сокета и токеном. Эти данные предназначены для desktop main process или headless-клиента, а не для renderer.

Одноразовый CLI покрывает проекты, миграции, extensions, sessions, operations,
approvals, artifacts, context, connectors, models и jobs. Pending
approval и непубликованные outbox candidates сохраняются между запусками CLI.
Секрет model provider передаётся только через `--secret-stdin`, а не аргументом
процесса.

Версии расширений устанавливаются side-by-side, а каждый открытый проект
получает собственный derived lock. Выданные расширению permissions и сохранённые
risk-policy rules находятся в локальном состоянии runtime, не в переносимой
папке проекта. Подложенный `.osnova/extensions/grants.json` не является
источником доверия.

`Reborn backend` CI прогоняет core, SDK, runtime/CLI, reference extensions и
desktop bridge на `macos-14` и `windows-2022`. Если репозитории организации
закрыты, для cross-repository checkout нужен read-only secret
`OSNOVA_REPO_TOKEN`. Для публичных репозиториев достаточно `github.token`.

## Связанные репозитории

- `osnova-spec` определяет формат проекта, RPC и Extension Manifest v1.
- `osnova-core` предоставляет project IO, manifest и validation APIs.
- `osnova-plugin-sdk` задаёт публичный контракт авторов расширений.
- `osnova-desktop` подключает runtime через main process и preload bridge.
- `osnova-plugins` содержит reference-расширения для runtime.
- `osnova-docs` содержит нормативную документацию архитектуры и политики
  доверия.

## Лицензия

Apache-2.0. Код Sentient OS не переносится в этот репозиторий.
