import { Download, Loader2 } from "lucide-react";

import { AdminLayout } from "@/components/AdminLayout";
import { DataTable } from "@/components/DataTable";
import { ErrorState } from "@/components/ErrorState";
import { LoadingState } from "@/components/LoadingState";
import { StatusBadge } from "@/components/StatusBadge";
import { api } from "@/lib/api";
import { useToast } from "@/hooks/use-toast";
import { useSessions } from "@/hooks/useApi";
import type { Session } from "@/types/api";
import { useState } from "react";

function ExportButton({ session }: { session: Session }) {
  const { toast } = useToast();
  const [isDownloading, setIsDownloading] = useState(false);

  const handleExport = async () => {
    try {
      setIsDownloading(true);
      const blob = await api.downloadSession(session.name);
      const url = window.URL.createObjectURL(blob);
      const anchor = document.createElement("a");
      anchor.href = url;
      anchor.download = `${session.name}.session`;
      document.body.appendChild(anchor);
      anchor.click();
      anchor.remove();
      window.URL.revokeObjectURL(url);

      toast({
        title: "Session exported",
        description: `${session.name}.session downloaded successfully`,
      });
    } catch (error) {
      toast({
        variant: "destructive",
        title: "Export failed",
        description: (error as Error).message,
      });
    } finally {
      setIsDownloading(false);
    }
  };

  return (
    <button
      onClick={handleExport}
      disabled={isDownloading}
      className="p-1.5 rounded-md hover:bg-surface-hover transition-colors text-muted-foreground hover:text-primary disabled:opacity-50 disabled:cursor-not-allowed"
      title="Download session file"
    >
      {isDownloading ? <Loader2 className="w-4 h-4 animate-spin" /> : <Download className="w-4 h-4" />}
    </button>
  );
}

const columns = [
  {
    key: "name",
    label: "Session",
    render: (row: Session) => (
      <div className="min-w-[220px]">
        <p className="font-mono text-primary text-xs">{row.name}</p>
        <p className="text-xs text-muted-foreground">
          {row.first_name || "-"} · {row.username || "-"}
        </p>
      </div>
    ),
  },
  {
    key: "owner",
    label: "Owner",
    render: (row: Session) => <span className="font-mono">{row.owner}</span>,
  },
  {
    key: "status",
    label: "Health",
    render: (row: Session) => <StatusBadge status={row.status} />,
  },
  {
    key: "state",
    label: "Lifecycle",
    render: (row: Session) => <StatusBadge status={row.state} />,
  },
  {
    key: "pool",
    label: "Pool",
    render: (row: Session) => <StatusBadge status={row.pool} />,
  },
  {
    key: "last_error",
    label: "Last error",
    render: (row: Session) => (
      <span className={row.last_error ? "font-mono text-xs text-destructive" : "text-muted-foreground text-xs"}>
        {row.last_error ?? "-"}
      </span>
    ),
  },
  {
    key: "actions",
    label: "",
    className: "w-12",
    render: (row: Session) => <ExportButton session={row} />,
  },
];

export default function SessionsPage() {
  const { data, isLoading, isError, error, refetch } = useSessions();

  return (
    <AdminLayout>
      <div className="space-y-6 animate-fade-in">
        <div>
          <h2 className="text-2xl font-semibold text-foreground">Sessions</h2>
          <p className="text-sm text-muted-foreground mt-1">
            Session health, lifecycle state and direct export from admin API.
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
            getRowKey={(row) => String((row as Session).id)}
          />
        )}
      </div>
    </AdminLayout>
  );
}
