import { AdminLayout } from "@/components/AdminLayout";
import { DataTable } from "@/components/DataTable";
import { ErrorState } from "@/components/ErrorState";
import { LoadingState } from "@/components/LoadingState";
import { StatusBadge } from "@/components/StatusBadge";
import { useUsers } from "@/hooks/useApi";
import type { User } from "@/types/api";

const columns = [
  {
    key: "telegram_id",
    label: "Telegram ID",
    render: (row: User) => <span className="font-mono text-primary">{row.telegram_id}</span>,
  },
  {
    key: "display_name",
    label: "Profile",
    render: (row: User) => (
      <div className="min-w-[220px]">
        <p className="font-medium">{row.display_name}</p>
        <p className="text-xs text-muted-foreground">Scope: {row.owner_scope_id}</p>
      </div>
    ),
  },
  {
    key: "role",
    label: "Role",
    render: (row: User) => <StatusBadge status={row.role} />,
  },
  {
    key: "status",
    label: "Status",
    render: (row: User) => <StatusBadge status={row.status} />,
  },
  {
    key: "tariff",
    label: "Tariff",
    render: (row: User) => <span className="font-mono text-xs uppercase">{row.tariff}</span>,
  },
  {
    key: "sessions",
    label: "Sessions",
  },
  {
    key: "key_issued",
    label: "Latest key",
    render: (row: User) => (
      <div className="text-xs font-mono text-muted-foreground min-w-[150px]">
        <div>{row.key_issued ?? "-"}</div>
        <div>{row.key_expires ? `exp: ${row.key_expires}` : "no expiry"}</div>
      </div>
    ),
  },
  {
    key: "online",
    label: "Online",
    render: (row: User) => (
      <span
        className={`inline-block w-2.5 h-2.5 rounded-full ${
          row.online ? "bg-success" : "bg-muted-foreground"
        }`}
      />
    ),
  },
];

export default function UsersPage() {
  const { data, isLoading, isError, error, refetch } = useUsers();

  return (
    <AdminLayout>
      <div className="space-y-6 animate-fade-in">
        <div>
          <h2 className="text-2xl font-semibold text-foreground">Users</h2>
          <p className="text-sm text-muted-foreground mt-1">
            Access registry, roles and session visibility.
          </p>
        </div>

        {isError ? (
          <ErrorState message={(error as Error)?.message} onRetry={() => refetch()} />
        ) : isLoading ? (
          <LoadingState rows={6} columns={8} />
        ) : (
          <DataTable
            columns={columns}
            data={(data ?? []) as unknown as Record<string, unknown>[]}
            getRowKey={(row) => String((row as User).telegram_id)}
          />
        )}
      </div>
    </AdminLayout>
  );
}
