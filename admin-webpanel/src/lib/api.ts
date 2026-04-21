import type {
  AuditResponse,
  Command,
  CommandResponse,
  DashboardStats,
  HealthResponse,
  KeysResponse,
  SessionsResponse,
  TasksResponse,
  UsersResponse,
} from "@/types/api";
import { clearAdminApiToken, getAdminApiToken } from "@/lib/auth";

const BASE_URL = (import.meta.env.VITE_API_URL as string) || "http://localhost:8000";

class ApiError extends Error {
  status: number;

  constructor(message: string, status: number) {
    super(message);
    this.status = status;
    this.name = "ApiError";
  }
}

async function request<T>(path: string, options?: RequestInit): Promise<T> {
  const token = getAdminApiToken();
  const response = await fetch(`${BASE_URL}${path}`, {
    headers: {
      "Content-Type": "application/json",
      ...(token ? { Authorization: `Bearer ${token}` } : {}),
      ...options?.headers,
    },
    ...options,
  });

  if (!response.ok) {
    if (response.status === 401 && typeof window !== "undefined") {
      clearAdminApiToken();
      if (window.location.pathname !== "/login") {
        window.location.assign("/login");
      }
    }
    let message = `HTTP ${response.status}`;
    try {
      const body = await response.json();
      message = body?.detail ?? body?.message ?? message;
    } catch {
      // Keep the plain HTTP status text fallback.
    }
    throw new ApiError(message, response.status);
  }

  return response.json() as Promise<T>;
}

async function download(path: string): Promise<Blob> {
  const token = getAdminApiToken();
  const response = await fetch(`${BASE_URL}${path}`, {
    headers: {
      ...(token ? { Authorization: `Bearer ${token}` } : {}),
    },
  });
  if (!response.ok) {
    if (response.status === 401 && typeof window !== "undefined") {
      clearAdminApiToken();
      if (window.location.pathname !== "/login") {
        window.location.assign("/login");
      }
    }
    throw new ApiError(`HTTP ${response.status}`, response.status);
  }
  return response.blob();
}

async function verifyToken(token: string): Promise<void> {
  const response = await fetch(`${BASE_URL}/auth/check`, {
    headers: {
      Authorization: `Bearer ${token.trim()}`,
    },
  });

  if (!response.ok) {
    let message = `HTTP ${response.status}`;
    try {
      const body = await response.json();
      message = body?.detail ?? body?.message ?? message;
    } catch {
      // Ignore JSON parse errors.
    }
    throw new ApiError(message, response.status);
  }
}

export const api = {
  getHealth: () => request<HealthResponse>("/health"),
  verifyToken,
  getDashboard: () => request<DashboardStats>("/dashboard"),
  getUsers: () => request<UsersResponse>("/users"),
  getKeys: () => request<KeysResponse>("/keys"),
  getSessions: () => request<SessionsResponse>("/sessions"),
  getTasks: () => request<TasksResponse>("/tasks"),
  getAudit: () => request<AuditResponse>("/audit"),
  downloadSession: (sessionId: string | number) =>
    download(`/sessions/${encodeURIComponent(String(sessionId))}/export`),
  sendCommand: (command: Command) =>
    request<CommandResponse>("/commands", {
      method: "POST",
      body: JSON.stringify(command),
    }),
};

export { ApiError, BASE_URL };
