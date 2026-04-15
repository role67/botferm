import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";

import { api } from "@/lib/api";
import type { Command } from "@/types/api";

export const QUERY_KEYS = {
  health: ["health"] as const,
  dashboard: ["dashboard"] as const,
  users: ["users"] as const,
  keys: ["keys"] as const,
  sessions: ["sessions"] as const,
  tasks: ["tasks"] as const,
  audit: ["audit"] as const,
};

export function useHealth() {
  return useQuery({
    queryKey: QUERY_KEYS.health,
    queryFn: api.getHealth,
    staleTime: 15_000,
    refetchInterval: 30_000,
    retry: 1,
  });
}

export function useDashboard() {
  return useQuery({
    queryKey: QUERY_KEYS.dashboard,
    queryFn: api.getDashboard,
    staleTime: 10_000,
    refetchInterval: 15_000,
  });
}

export function useUsers() {
  return useQuery({
    queryKey: QUERY_KEYS.users,
    queryFn: api.getUsers,
    staleTime: 30_000,
  });
}

export function useKeys() {
  return useQuery({
    queryKey: QUERY_KEYS.keys,
    queryFn: api.getKeys,
    staleTime: 30_000,
  });
}

export function useSessions() {
  return useQuery({
    queryKey: QUERY_KEYS.sessions,
    queryFn: api.getSessions,
    staleTime: 20_000,
    refetchInterval: 20_000,
  });
}

export function useTasks() {
  return useQuery({
    queryKey: QUERY_KEYS.tasks,
    queryFn: api.getTasks,
    refetchInterval: 4_000,
    staleTime: 0,
  });
}

export function useAudit() {
  return useQuery({
    queryKey: QUERY_KEYS.audit,
    queryFn: api.getAudit,
    staleTime: 20_000,
    refetchInterval: 20_000,
  });
}

export function useSendCommand() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: (command: Command) => api.sendCommand(command),
    onSuccess: (_data, command) => {
      switch (command.type) {
        case "export_session":
          queryClient.invalidateQueries({ queryKey: QUERY_KEYS.sessions });
          queryClient.invalidateQueries({ queryKey: QUERY_KEYS.audit });
          break;
        case "pause_task":
        case "resume_task":
        case "cancel_task":
        case "remove_task":
        case "clear_finished":
          queryClient.invalidateQueries({ queryKey: QUERY_KEYS.tasks });
          queryClient.invalidateQueries({ queryKey: QUERY_KEYS.audit });
          break;
        default:
          queryClient.invalidateQueries();
      }
    },
  });
}
