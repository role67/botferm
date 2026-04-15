import { InboxIcon } from "lucide-react";

interface Column<T> {
  key: string;
  label: string;
  className?: string;
  render?: (row: T) => React.ReactNode;
}

interface DataTableProps<T> {
  columns: Column<T>[];
  data: T[];
  title?: string;
  emptyMessage?: string;
  getRowKey?: (row: T, index: number) => React.Key;
}

export function DataTable<T extends Record<string, unknown>>({
  columns,
  data,
  title,
  emptyMessage = "No data available",
  getRowKey,
}: DataTableProps<T>) {
  return (
    <div className="glass-panel rounded-xl overflow-hidden">
      {title ? (
        <div className="px-5 py-4 border-b border-border">
          <h3 className="text-sm font-semibold text-foreground">{title}</h3>
        </div>
      ) : null}

      <div className="overflow-x-auto">
        <table className="w-full">
          <thead>
            <tr className="border-b border-border">
              {columns.map((col) => (
                <th
                  key={col.key}
                  className="px-5 py-3 text-left text-xs font-medium text-muted-foreground uppercase tracking-wider"
                >
                  {col.label}
                </th>
              ))}
            </tr>
          </thead>

          <tbody>
            {data.length === 0 ? (
              <tr>
                <td colSpan={columns.length} className="px-5 py-10 text-center">
                  <div className="flex flex-col items-center gap-2 text-muted-foreground">
                    <InboxIcon className="w-8 h-8 opacity-30" />
                    <span className="text-xs">{emptyMessage}</span>
                  </div>
                </td>
              </tr>
            ) : (
              data.map((row, index) => (
                <tr
                  key={getRowKey ? getRowKey(row, index) : index}
                  className="border-b border-border/50 table-row-hover transition-colors align-top"
                >
                  {columns.map((col) => (
                    <td
                      key={col.key}
                      className={`px-5 py-3.5 text-sm text-foreground ${col.className ?? ""}`}
                    >
                      {col.render ? col.render(row) : String(row[col.key] ?? "")}
                    </td>
                  ))}
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}
