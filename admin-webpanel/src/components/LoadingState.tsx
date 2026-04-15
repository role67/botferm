interface LoadingStateProps {
  rows?: number;
  columns?: number;
}

export function LoadingState({ rows = 5, columns = 4 }: LoadingStateProps) {
  return (
    <div className="glass-panel rounded-xl overflow-hidden animate-pulse">
      <div className="overflow-x-auto">
        <table className="w-full">
          <thead>
            <tr className="border-b border-border">
              {Array.from({ length: columns }).map((_, i) => (
                <th key={i} className="px-5 py-3">
                  <div className="h-3 bg-muted/40 rounded w-20" />
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {Array.from({ length: rows }).map((_, rowIdx) => (
              <tr key={rowIdx} className="border-b border-border/50">
                {Array.from({ length: columns }).map((_, colIdx) => (
                  <td key={colIdx} className="px-5 py-3.5">
                    <div
                      className="h-3 bg-muted/30 rounded"
                      style={{ width: `${55 + ((rowIdx * 13 + colIdx * 17) % 40)}%` }}
                    />
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

export function StatLoadingState({ count = 4 }: { count?: number }) {
  return (
    <div
      className="grid gap-4"
      style={{ gridTemplateColumns: `repeat(${count}, minmax(0, 1fr))` }}
    >
      {Array.from({ length: count }).map((_, i) => (
        <div key={i} className="stat-card animate-pulse">
          <div className="flex items-start justify-between mb-3">
            <div className="w-10 h-10 rounded-lg bg-muted/30" />
          </div>
          <div className="h-7 bg-muted/40 rounded w-16 mb-2" />
          <div className="h-3 bg-muted/30 rounded w-28" />
        </div>
      ))}
    </div>
  );
}
