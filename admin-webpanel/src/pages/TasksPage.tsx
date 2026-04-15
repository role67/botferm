import { Loader2, Pause, Play, RefreshCw, RotateCcw, Trash2, XCircle } from "lucide-react";

import { AdminLayout } from "@/components/AdminLayout";
import { DataTable } from "@/components/DataTable";
import { ErrorState } from "@/components/ErrorState";
import { LoadingState } from "@/components/LoadingState";
import { StatusBadge } from "@/components/StatusBadge";
import { useSendCommand, useTasks } from "@/hooks/useApi";
import { useToast } from "@/hooks/use-toast";
import type { Task } from "@/types/api";

function TaskActionButton({
  icon: Icon,
  onClick,
  title,
  disabled,
}: {
  icon: typeof Pause;
  onClick: () => void;
  title: string;
  disabled?: boolean;
}) {
  return (
    <button
      onClick={onClick}
      disabled={disabled}
      title={title}
      className="p-1.5 rounded-md hover:bg-surface-hover transition-colors text-muted-foreground hover:text-primary disabled:opacity-50 disabled:cursor-not-allowed"
    >
      <Icon className="w-4 h-4" />
    </button>
  );
}

function TaskActions({ task }: { task: Task }) {
  const { toast } = useToast();
  const { mutate, isPending } = useSendCommand();

  const send = (type: string) => {
    mutate(
      {
        type,
        data: { task_id: Number(task.id) },
      },
      {
        onSuccess: (result) => {
          toast({
            title: "Task updated",
            description: result.message ?? `${type} completed`,
          });
        },
        onError: (error) => {
          toast({
            variant: "destructive",
            title: "Command failed",
            description: (error as Error).message,
          });
        },
      },
    );
  };

  return (
    <div className="flex items-center gap-1">
      {task.status === "running" ? (
        <TaskActionButton icon={Pause} onClick={() => send("pause_task")} title="Pause task" disabled={isPending} />
      ) : null}
      {task.status === "paused" ? (
        <TaskActionButton icon={RotateCcw} onClick={() => send("resume_task")} title="Resume task" disabled={isPending} />
      ) : null}
      {["queued", "paused", "running", "cancel_requested"].includes(task.status) ? (
        <TaskActionButton icon={XCircle} onClick={() => send("cancel_task")} title="Cancel task" disabled={isPending} />
      ) : null}
      {["completed", "failed", "canceled"].includes(task.status) ? (
        <TaskActionButton icon={Trash2} onClick={() => send("remove_task")} title="Remove task" disabled={isPending} />
      ) : null}
    </div>
  );
}

const columns = [
  {
    key: "id",
    label: "ID",
    render: (row: Task) => <span className="font-mono text-primary text-xs">{row.id}</span>,
  },
  {
    key: "type",
    label: "Type",
    render: (row: Task) => (
      <div className="min-w-[180px]">
        <p className="font-medium">{row.type}</p>
        <p className="text-xs text-muted-foreground">{row.title}</p>
      </div>
    ),
  },
  {
    key: "started_by",
    label: "Requested by",
    render: (row: Task) => <span className="font-mono">{row.started_by}</span>,
  },
  {
    key: "status",
    label: "Status",
    render: (row: Task) => <StatusBadge status={row.status} />,
  },
  {
    key: "started_at",
    label: "Started",
    render: (row: Task) => <span className="font-mono text-xs">{row.started_at}</span>,
  },
  {
    key: "finished_at",
    label: "Finished",
    render: (row: Task) => <span className="font-mono text-xs">{row.finished_at ?? "-"}</span>,
  },
  {
    key: "progress",
    label: "Progress",
    render: (row: Task) => (
      <div className="min-w-[260px] text-xs">
        <p className="text-foreground">{row.progress ?? "-"}</p>
        <p className="text-muted-foreground">{row.result ?? "-"}</p>
      </div>
    ),
  },
  {
    key: "actions",
    label: "",
    render: (row: Task) => <TaskActions task={row} />,
  },
];

export default function TasksPage() {
  const { data, isLoading, isError, error, refetch, isFetching } = useTasks();
  const { toast } = useToast();
  const { mutate, isPending } = useSendCommand();

  const clearFinished = () => {
    mutate(
      {
        type: "clear_finished",
        data: {},
      },
      {
        onSuccess: (result) => {
          toast({
            title: "Task history cleaned",
            description: result.message ?? "Finished tasks removed",
          });
        },
        onError: (err) => {
          toast({
            variant: "destructive",
            title: "Cleanup failed",
            description: (err as Error).message,
          });
        },
      },
    );
  };

  return (
    <AdminLayout>
      <div className="space-y-6 animate-fade-in">
        <div className="flex items-start justify-between gap-4">
          <div>
            <h2 className="text-2xl font-semibold text-foreground">Tasks</h2>
            <p className="text-sm text-muted-foreground mt-1">
              Queue state, progress tracking and task control from the admin panel.
              {isFetching && !isLoading ? (
                <span className="ml-2 inline-flex items-center gap-1 text-info">
                  <RefreshCw className="w-3 h-3 animate-spin" />
                  updating
                </span>
              ) : null}
            </p>
          </div>

          <button
            onClick={clearFinished}
            disabled={isPending}
            className="flex items-center gap-2 h-9 px-4 rounded-lg text-sm font-medium bg-primary/10 hover:bg-primary/20 text-primary border border-primary/20 transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
          >
            {isPending ? <Loader2 className="w-3.5 h-3.5 animate-spin" /> : <Play className="w-3.5 h-3.5" />}
            Clear finished
          </button>
        </div>

        {isError ? (
          <ErrorState message={(error as Error)?.message} onRetry={() => refetch()} />
        ) : isLoading ? (
          <LoadingState rows={6} columns={8} />
        ) : (
          <DataTable
            columns={columns}
            data={(data ?? []) as unknown as Record<string, unknown>[]}
            getRowKey={(row) => (row as Task).id}
          />
        )}
      </div>
    </AdminLayout>
  );
}
