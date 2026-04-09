export type LLMProvider =
  | 'openai'
  | 'nvidia'
  | 'anthropic'
  | 'openrouter'
  | 'ollama'
  | 'custom';

export interface LLMProviderOption {
  id: LLMProvider;
  label: string;
  description: string;
  default_model: string;
  default_base_url?: string | null;
  api_key_required: boolean;
  protocol: string;
}

export interface LLMProfile {
  id: string;
  display_name: string;
  provider: LLMProvider;
  model: string;
  base_url?: string | null;
  has_api_key: boolean;
  api_key_masked?: string | null;
  updated_at?: string | null;
}

export interface LLMSettings {
  active_profile_id?: string | null;
  fallback_profile_id?: string | null;
  profiles: LLMProfile[];
  providers: LLMProviderOption[];
}

export interface LLMProfileUpsert {
  id?: string | null;
  display_name: string;
  provider: LLMProvider;
  model: string;
  base_url?: string | null;
  api_key?: string | null;
}

export interface LLMRoutingUpdate {
  primary_profile_id: string;
  fallback_profile_id?: string | null;
}

export interface LLMTestResult {
  success: boolean;
  provider: LLMProvider;
  model: string;
  latency_ms: number;
  message: string;
}
