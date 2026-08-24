# osnova-runtime

Локальный control plane Osnova Reborn. Он управляет проектами, расширениями, операциями, заданиями, контекстом, моделями и диалоговым агентом с инструментами. AI и OCI являются опциональными возможностями: проект открывается и остаётся полезным без них.

## Границы

- Runtime не владеет пользовательскими данными: долговечное состояние хранится в открытой папке проекта.
- Extension-инструменты не получают прямой доступ на запись к проекту. Результаты
  проходят через outbox и `ArtifactIngestor`; привилегированные built-ins пишут
  только через атомарные API `osnova-core` и возвращают artifact ids в тот же job.
- Агент вызывает только зарегистрированные операции и не получает shell/filesystem API.
- Локальный RPC использует случайный адрес и bearer-токен экземпляра.

## Команды

```bash
npm install --ignore-scripts
npm run build
npm test
node dist/cli.js selftest
node dist/cli.js serve
node dist/cli.js help
```

Запуск `serve` печатает JSON с адресом сокета и токеном. Эти данные предназначены для desktop main process или headless-клиента, а не для renderer.

Одноразовый CLI покрывает проекты, миграции, extensions, sessions, operations,
approvals, artifacts, context, connectors, models и jobs. Pending
approval и непубликованные outbox candidates сохраняются между запусками CLI.
Секрет model provider передаётся только через `--secret-stdin`, а не аргументом
процесса.

Версии расширений устанавливаются side-by-side, а каждый открытый проект
получает собственный derived lock. Выданные расширению permissions и сохранённые
risk-policy rules находятся в локальном состоянии runtime, не в переносимой
папке проекта; подложенный `.osnova/extensions/grants.json` не является
источником доверия.

`Reborn backend` CI прогоняет core, SDK, runtime/CLI, reference extensions и
desktop bridge на `macos-14` и `windows-2022`. Если репозитории организации
закрыты, для cross-repository checkout нужен read-only secret
`OSNOVA_REPO_TOKEN`; для публичных репозиториев достаточно `github.token`.

## Лицензия

Apache-2.0. Код Sentient OS не переносится в этот репозиторий.
