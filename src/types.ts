/**
 * Тип-only публичный вход osnova-runtime.
 * Канонические DTO рантайма, реэкспортированные из мест объявления,
 * чтобы consumer'ы (renderer osnova-desktop и др.) не дублировали их.
 * Только типы: в бандл рантайм-код не попадает.
 */
export type {
  TokenCountSource,
  ModelGenerationMetrics,
  ChatRunMetrics,
  ChatRun,
  AgentKernelActivity
} from "./agent-kernel.js";
export type {
  AgentOutputDelta,
  AgentActivity,
  ModelProviderModelCatalog
} from "./agent-orchestrator.js";
export type { InstalledExtension } from "./extension-manager.js";
export type { RegisteredOperation } from "./operation-registry.js";
export type { ModelProviderModel } from "./model-provider.js";
export type { ModelProviderConfig } from "./runtime.js";
