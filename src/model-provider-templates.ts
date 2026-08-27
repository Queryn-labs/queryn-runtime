import type { ModelProviderTemplate, RecipientKind } from "@queryn/types";

const MODEL_PROVIDER_TEMPLATES: readonly ModelProviderTemplate[] = [
  {
    id: "local.ollama",
    group: "local",
    displayName: "Ollama",
    description: "Локальные модели через установленный сервер Ollama.",
    transport: "openai-compatible",
    auth: "none",
    defaultProviderId: "local.ollama",
    defaultEndpoint: "http://127.0.0.1:11434/v1"
  },
  {
    id: "local.lm-studio",
    group: "local",
    displayName: "LM Studio",
    description: "Локальные модели через сервер LM Studio.",
    transport: "openai-compatible",
    auth: "none",
    defaultProviderId: "local.lm-studio",
    defaultEndpoint: "http://127.0.0.1:1234/v1"
  },
  {
    id: "local.llama-cpp",
    group: "local",
    displayName: "llama.cpp",
    description: "Локальные модели через внешний сервер llama.cpp.",
    transport: "openai-compatible",
    auth: "none",
    defaultProviderId: "local.llama-cpp",
    defaultEndpoint: "http://127.0.0.1:8080/v1"
  },
  {
    id: "local.openai-compatible",
    group: "local",
    displayName: "Другой локальный сервер",
    description: "Произвольный локальный сервер с OpenAI-совместимым интерфейсом.",
    transport: "openai-compatible",
    auth: "none",
    defaultProviderId: "local.custom"
  },
  {
    id: "cloud.openai",
    group: "cloud",
    displayName: "OpenAI",
    description: "Облачные модели OpenAI по ключу доступа.",
    transport: "openai-compatible",
    auth: "api-key",
    defaultProviderId: "cloud.openai",
    defaultEndpoint: "https://api.openai.com/v1"
  },
  {
    id: "cloud.google-gemini",
    group: "cloud",
    displayName: "Google Gemini",
    description: "Модели Gemini через OpenAI-совместимый интерфейс Google.",
    transport: "openai-compatible",
    auth: "api-key",
    defaultProviderId: "cloud.google-gemini",
    defaultEndpoint: "https://generativelanguage.googleapis.com/v1beta/openai"
  },
  {
    id: "cloud.mistral",
    group: "cloud",
    displayName: "Mistral AI",
    description: "Облачные модели Mistral по ключу доступа.",
    transport: "openai-compatible",
    auth: "api-key",
    defaultProviderId: "cloud.mistral",
    defaultEndpoint: "https://api.mistral.ai/v1"
  },
  {
    id: "cloud.deepseek",
    group: "cloud",
    displayName: "DeepSeek",
    description: "Облачные модели DeepSeek по ключу доступа.",
    transport: "openai-compatible",
    auth: "api-key",
    defaultProviderId: "cloud.deepseek",
    defaultEndpoint: "https://api.deepseek.com"
  },
  {
    id: "cloud.groq",
    group: "cloud",
    displayName: "Groq",
    description: "Облачный вывод моделей через Groq.",
    transport: "openai-compatible",
    auth: "api-key",
    defaultProviderId: "cloud.groq",
    defaultEndpoint: "https://api.groq.com/openai/v1"
  },
  {
    id: "cloud.xai",
    group: "cloud",
    displayName: "xAI",
    description: "Облачные модели xAI по ключу доступа.",
    transport: "openai-compatible",
    auth: "api-key",
    defaultProviderId: "cloud.xai",
    defaultEndpoint: "https://api.x.ai/v1"
  },
  {
    id: "cloud.openrouter",
    group: "cloud",
    displayName: "OpenRouter",
    description: "Единый облачный доступ к моделям разных поставщиков.",
    transport: "openai-compatible",
    auth: "api-key",
    defaultProviderId: "cloud.openrouter",
    defaultEndpoint: "https://openrouter.ai/api/v1"
  },
  {
    id: "cloud.together",
    group: "cloud",
    displayName: "Together AI",
    description: "Облачный вывод открытых моделей через Together AI.",
    transport: "openai-compatible",
    auth: "api-key",
    defaultProviderId: "cloud.together",
    defaultEndpoint: "https://api.together.ai/v1"
  },
  {
    id: "cloud.fireworks",
    group: "cloud",
    displayName: "Fireworks AI",
    description: "Облачный вывод моделей через Fireworks AI.",
    transport: "openai-compatible",
    auth: "api-key",
    defaultProviderId: "cloud.fireworks",
    defaultEndpoint: "https://api.fireworks.ai/inference/v1"
  },
  {
    id: "cloud.openai-compatible",
    group: "cloud",
    displayName: "Другой облачный сервер",
    description: "Произвольный HTTPS-сервер с OpenAI-совместимым интерфейсом.",
    transport: "openai-compatible",
    auth: "api-key",
    defaultProviderId: "cloud.custom"
  }
];

const TEMPLATES_BY_ID = new Map(MODEL_PROVIDER_TEMPLATES.map((template) => [template.id, template]));

export function listModelProviderTemplates(): ModelProviderTemplate[] {
  return MODEL_PROVIDER_TEMPLATES.map((template) => ({ ...template }));
}

export function findModelProviderTemplate(id: string): ModelProviderTemplate | undefined {
  return TEMPLATES_BY_ID.get(id);
}

export function inferModelProviderTemplateId(recipient: RecipientKind, endpoint: string): string {
  const normalizedEndpoint = normalizeEndpoint(endpoint);
  const matched = MODEL_PROVIDER_TEMPLATES.find((template) =>
    template.group === recipient && template.defaultEndpoint !== undefined && normalizeEndpoint(template.defaultEndpoint) === normalizedEndpoint
  );
  return matched?.id ?? `${recipient}.openai-compatible`;
}

function normalizeEndpoint(endpoint: string): string {
  const url = new URL(endpoint);
  if (["127.0.0.1", "localhost", "[::1]"].includes(url.hostname)) url.hostname = "127.0.0.1";
  url.hash = "";
  url.search = "";
  url.pathname = url.pathname.replace(/\/+$/, "");
  return url.toString().replace(/\/$/, "");
}
