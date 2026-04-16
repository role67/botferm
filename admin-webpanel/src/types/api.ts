export interface HealthResponse {
  ok: boolean;
  uptime_seconds: number;
  logs: Array<{
    name: string;
    path: string;
    size_bytes: number;
    updated_at: number;
  }>;
}

export interface DashboardStats {
  total_users: number;
  total_keys: number;
  total_sessions: number;
  total_tasks: number;
  sessions_alive: number;
  sessions_limited: number;
  sessions_banned: number;
  sessions_in_pool: number;
  sessions_out_pool: number;
  tasks_active: number;
  tasks_done_today: number;
  uptime_seconds: number;
}

export interface User {
  telegram_id: string;
  display_name: string;
  role: string;
  tariff: string;
  status: string;
  key_issued: string | null;
  key_expires: string | null;
  sessions: number;
  online: boolean;
  owner_scope_id: string;
  created_at: string | null;
  activated_at: string | null;
}

export type UsersResponse = User[];

export interface AccessKey {
  key: string;
  issued_to: string;
  role: string;
  tariff: string;
  issued_at: string | null;
  expires_at: string | null;
  status: string;
  activated_by_id: string | null;
}

export type KeysResponse = AccessKey[];

export interface Session {
  id: number | string;
  name: string;
  owner: string;
  status: string;
  state: string;
  pool: string;
  added: string | null;
  last_error: string | null;
  username: string;
  first_name: string;
  available_for_tasks: boolean;
}

export type SessionsResponse = Session[];

export type TaskStatus =
  | "queued"
  | "paused"
  | "running"
  | "cancel_requested"
  | "completed"
  | "failed"
  | "canceled";

export interface Task {
  id: string;
  type: string;
  title: string;
  started_by: string;
  status: TaskStatus;
  started_at: string;
  finished_at: string | null;
  result: string | null;
  progress: string | null;
  queue_position: number | null;
}

export type TasksResponse = Task[];

export interface AuditEntry {
  id?: number | string;
  action: string;
  who: string;
  target: string;
  timestamp: string;
  level: string;
  message: string;
}

export type AuditResponse = AuditEntry[];

export interface Command<T = Record<string, unknown>> {
  type: string;
  data: T;
}

export interface CommandResponse {
  ok: boolean;
  command_id?: string;
  message?: string;
  download_url?: string;
}

export interface ApiError {
  detail?: string;
  message?: string;
  status?: number;
}
