import { AdminLayout } from "@/components/AdminLayout";
import { DataTable } from "@/components/DataTable";
import { ErrorState } from "@/components/ErrorState";
import { LoadingState } from "@/components/LoadingState";
import { StatusBadge } from "@/components/StatusBadge";
import { useKeys } from "@/hooks/useApi";
import type { AccessKey } from "@/types/api";

const columns = [
  {
    key: "key",
    label: "Key",
    render: (row: AccessKey) => <span className="font-mono text-primary text-xs">{row.key}</span>,
  },
  {
    key: "issued_to",
    label: "Issued to",
    render: (row: AccessKey) => <span className="font-mono">{row.issued_to}</span>,
  },
  {
    key: "role",
    label: "Role",
    render: (row: AccessKey) => <StatusBadge status={row.role} />,
  },
  {
    key: "tariff",
    label: "Tariff",
    render: (row: AccessKey) => <span className="font-mono text-xs uppercase">{row.tariff}</span>,
  },
  {
    key: "issued_at",
    label: "Created",
    render: (row: AccessKey) => <span className="font-mono text-xs">{row.issued_at ?? "-"}</span>,
  },
  {
    key: "expires_at",
    label: "Expires",
    render: (row: AccessKey) => <span className="font-mono text-xs">{row.expires_at ?? "-"}</span>,
  },
  {
    key: "activated_by_id",
    label: "Activated by",
    render: (row: AccessKey) => <span className="font-mono text-xs">{row.activated_by_id ?? "-"}</span>,
  },
  {
    key: "status",
    label: "Status",
    render: (row: AccessKey) => <StatusBadge status={row.status} />,
  },
];

export default function KeysPage() {
  const { data, isLoading, isError, error, refetch } = useKeys();

  return (
    <AdminLayout>
      <div className="space-y-6 animate-fade-in">
        <div>
          <h2 className="text-2xl font-semibold text-foreground">Keys</h2>
          <p className="text-sm text-muted-foreground mt-1">
            Access key inventory with role and tariff metadata.
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
            getRowKey={(row) => (row as AccessKey).key}
          />
        )}
      </div>
    </AdminLayout>
  );
}
