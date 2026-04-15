const statusStyles: Record<string, string> = {
  active: "bg-success/15 text-success",
  alive: "bg-success/15 text-success",
  success: "bg-success/15 text-success",
  completed: "bg-success/15 text-success",
  available: "bg-success/15 text-success",
  info: "bg-info/15 text-info",
  limited: "bg-warning/15 text-warning",
  pending: "bg-warning/15 text-warning",
  paused: "bg-warning/15 text-warning",
  cancel_requested: "bg-warning/15 text-warning",
  warning: "bg-warning/15 text-warning",
  running: "bg-info/15 text-info",
  queued: "bg-info/15 text-info",
  in_pool: "bg-info/15 text-info",
  banned: "bg-destructive/15 text-destructive",
  dead: "bg-destructive/15 text-destructive",
  inactive: "bg-muted text-muted-foreground",
  error: "bg-destructive/15 text-destructive",
  failed: "bg-destructive/15 text-destructive",
  expired: "bg-destructive/15 text-destructive",
  critical: "bg-destructive/15 text-destructive",
  canceled: "bg-muted text-muted-foreground",
  out_of_pool: "bg-muted text-muted-foreground",
  owner: "bg-primary/15 text-primary",
  admin: "bg-primary/15 text-primary",
  internal: "bg-info/15 text-info",
  external: "bg-warning/15 text-warning",
};

function toLabel(status: string): string {
  return status
    .replaceAll("_", " ")
    .replace(/\b\w/g, (char) => char.toUpperCase());
}

export function StatusBadge({
  status,
  label,
}: {
  status: string;
  label?: string;
}) {
  const normalized = status.toLowerCase();
  const style = statusStyles[normalized] ?? "bg-muted text-muted-foreground";

  return (
    <span className={`inline-flex items-center px-2.5 py-1 rounded-md text-xs font-medium font-mono ${style}`}>
      {label ?? toLabel(normalized)}
    </span>
  );
}
