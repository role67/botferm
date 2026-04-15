import { Link, useLocation, useNavigate } from "react-router-dom";
import {
  Key,
  LayoutDashboard,
  ListTodo,
  Radio,
  Shield,
  Terminal,
  Users,
} from "lucide-react";

import { useHealth } from "@/hooks/useApi";
import { clearAdminApiToken } from "@/lib/auth";
import { Button } from "@/components/ui/button";

const navItems = [
  { label: "Dashboard", icon: LayoutDashboard, path: "/" },
  { label: "Users", icon: Users, path: "/users" },
  { label: "Keys", icon: Key, path: "/keys" },
  { label: "Sessions", icon: Radio, path: "/sessions" },
  { label: "Tasks", icon: ListTodo, path: "/tasks" },
  { label: "Audit", icon: Shield, path: "/audit" },
];

function ConnectionStatus() {
  const { data, isError, isLoading } = useHealth();

  if (isLoading) {
    return (
      <div className="flex items-center gap-1.5">
        <span className="inline-block w-1.5 h-1.5 rounded-full bg-warning animate-pulse" />
        <span className="text-xs text-muted-foreground">Connecting...</span>
      </div>
    );
  }

  if (isError || !data?.ok) {
    return (
      <div className="flex items-center gap-1.5">
        <span className="inline-block w-1.5 h-1.5 rounded-full bg-destructive" />
        <span className="text-xs text-destructive">API unavailable</span>
      </div>
    );
  }

  return (
    <div className="flex items-center gap-1.5">
      <span className="inline-block w-1.5 h-1.5 rounded-full bg-success" />
      <span className="text-xs text-muted-foreground">API connected</span>
    </div>
  );
}

export function AdminSidebar() {
  const location = useLocation();
  const navigate = useNavigate();

  return (
    <aside className="w-64 min-h-screen flex flex-col border-r border-border bg-sidebar shrink-0">
      <div className="flex items-center gap-3 px-5 py-5 border-b border-border">
        <div className="w-9 h-9 rounded-lg bg-primary/20 flex items-center justify-center">
          <Terminal className="w-5 h-5 text-primary" />
        </div>
        <div>
          <h1 className="text-sm font-semibold text-foreground">Botoferma Admin</h1>
          <p className="text-xs text-muted-foreground">Operations console</p>
        </div>
      </div>

      <nav className="flex-1 px-3 py-4 space-y-1 overflow-y-auto">
        {navItems.map((item) => {
          const Icon = item.icon;
          const active = location.pathname === item.path;

          return (
            <Link
              key={item.path}
              to={item.path}
              className={`flex items-center gap-3 px-3 py-2.5 rounded-lg text-sm transition-all duration-200 ${
                active
                  ? "bg-primary/10 text-primary"
                  : "text-sidebar-foreground hover:bg-surface-hover hover:text-foreground"
              }`}
            >
              <Icon className="w-4 h-4 shrink-0" />
              <span>{item.label}</span>
            </Link>
          );
        })}
      </nav>

      <div className="px-5 py-4 border-t border-border space-y-1">
        <ConnectionStatus />
        <p className="text-xs text-muted-foreground/50">Local admin API</p>
        <Button
          type="button"
          variant="outline"
          size="sm"
          className="mt-3 w-full"
          onClick={() => {
            clearAdminApiToken();
            navigate("/login");
          }}
        >
          Log out
        </Button>
      </div>
    </aside>
  );
}
