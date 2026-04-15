import { Activity, Key, ListTodo, Radio, Timer, Users, Zap } from "lucide-react";

import { AdminLayout } from "@/components/AdminLayout";
import { ErrorState } from "@/components/ErrorState";
import { StatLoadingState } from "@/components/LoadingState";
import { StatCard } from "@/components/StatCard";
import { useDashboard } from "@/hooks/useApi";

function formatUptime(seconds: number): string {
  const hours = Math.floor(seconds / 3600);
  const minutes = Math.floor((seconds % 3600) / 60);
  return `${hours}h ${minutes}m`;
}

export default function DashboardPage() {
  const { data, isLoading, isError, error, refetch } = useDashboard();

  return (
    <AdminLayout>
      <div className="space-y-8 animate-fade-in">
        <div>
          <h2 className="text-2xl font-semibold text-foreground glow-text">Dashboard</h2>
          <p className="text-sm text-muted-foreground mt-1">
            Live system metrics from the bot runtime and admin API.
          </p>
        </div>

        {isError ? (
          <ErrorState message={(error as Error)?.message} onRetry={() => refetch()} />
        ) : isLoading || !data ? (
          <>
            <StatLoadingState count={4} />
            <StatLoadingState count={5} />
            <StatLoadingState count={3} />
          </>
        ) : (
          <>
            <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
              <StatCard label="Total users" value={data.total_users} icon={Users} />
              <StatCard label="Access keys" value={data.total_keys} icon={Key} />
              <StatCard label="Sessions" value={data.total_sessions} icon={Radio} />
              <StatCard label="Tasks in history" value={data.total_tasks} icon={ListTodo} />
            </div>

            <div className="grid grid-cols-1 sm:grid-cols-3 lg:grid-cols-5 gap-4">
              <StatCard label="Alive" value={data.sessions_alive} icon={Activity} variant="success" />
              <StatCard label="Limited" value={data.sessions_limited} icon={Activity} variant="warning" />
              <StatCard label="Banned" value={data.sessions_banned} icon={Activity} variant="destructive" />
              <StatCard label="In pool" value={data.sessions_in_pool} icon={Zap} variant="info" />
              <StatCard label="Out of pool" value={data.sessions_out_pool} icon={Zap} />
            </div>

            <div className="grid grid-cols-1 sm:grid-cols-3 gap-4">
              <StatCard label="Active tasks" value={data.tasks_active} icon={ListTodo} variant="info" />
              <StatCard label="Tasks completed today" value={data.tasks_done_today} icon={ListTodo} variant="success" />
              <StatCard label="Runtime uptime" value={formatUptime(data.uptime_seconds)} icon={Timer} />
            </div>
          </>
        )}
      </div>
    </AdminLayout>
  );
}
