import { AlertTriangle, RefreshCw } from "lucide-react";

interface ErrorStateProps {
  message?: string;
  onRetry?: () => void;
}

export function ErrorState({ message, onRetry }: ErrorStateProps) {
  return (
    <div className="glass-panel rounded-xl p-10 flex flex-col items-center justify-center text-center gap-4">
      <div className="w-12 h-12 rounded-full bg-destructive/10 flex items-center justify-center">
        <AlertTriangle className="w-6 h-6 text-destructive" />
      </div>

      <div>
        <p className="text-sm font-medium text-foreground">Failed to load data</p>
        {message ? (
          <p className="text-xs text-muted-foreground mt-1 font-mono">{message}</p>
        ) : null}
      </div>

      {onRetry ? (
        <button
          onClick={onRetry}
          className="flex items-center gap-2 px-4 py-2 rounded-lg text-xs font-medium bg-surface hover:bg-surface-hover text-foreground transition-colors border border-border"
        >
          <RefreshCw className="w-3.5 h-3.5" />
          Retry
        </button>
      ) : null}
    </div>
  );
}
