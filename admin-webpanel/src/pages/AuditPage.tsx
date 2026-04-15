import { AdminLayout } from "@/components/AdminLayout";
import { DataTable } from "@/components/DataTable";
import { ErrorState } from "@/components/ErrorState";
import { LoadingState } from "@/components/LoadingState";
import { StatusBadge } from "@/components/StatusBadge";
import { useAudit } from "@/hooks/useApi";
import type { AuditEntry } from "@/types/api";

const columns = [
  {
    key: "timestamp",
    label: "Timestamp",
    render: (row: AuditEntry) => (
      <span className="font-mono text-xs text-muted-foreground">{row.timestamp}</span>
    ),
  },
  {
    key: "action",
    label: "Action",
    render: (row: AuditEntry) => (
      <div className="min-w-[240px]">
        <p className="text-sm font-medium text-foreground">{row.action}</p>
        <p className="text-xs text-muted-foreground">{row.message}</p>
      </div>
    ),
  },
  {
    key: "who",
    label: "Actor",
    render: (row: AuditEntry) => <span className="font-mono text-primary">{row.who}</span>,
  },
  {
    key: "target",
    label: "Target",
    render: (row: AuditEntry) => <span className="font-mono text-xs">{row.target}</span>,
  },
  {
    key: "level",
    label: "Level",
    render: (row: AuditEntry) => <StatusBadge status={row.level.toLowerCase()} label={row.level} />,
  },
];

export default function AuditPage() {
  const { data, isLoading, isError, error, refetch } = useAudit();

  return (
    <AdminLayout>
      <div className="space-y-6 animate-fade-in">
        <div>
          <h2 className="text-2xl font-semibold text-foreground">Audit</h2>
          <p className="text-sm text-muted-foreground mt-1">
            Recent structured events from the runtime audit log.
          </p>
        </div>

        {isError ? (
          <ErrorState message={(error as Error)?.message} onRetry={() => refetch()} />
        ) : isLoading ? (
          <LoadingState rows={8} columns={5} />
        ) : (
          <DataTable
            columns={columns}
            data={(data ?? []) as unknown as Record<string, unknown>[]}
            getRowKey={(row, index) => (row as AuditEntry).id ?? index}
          />
        )}
      </div>
    </AdminLayout>
  );
}
