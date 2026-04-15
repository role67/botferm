import { useState } from "react";

import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";

type LoginPageProps = {
  onSubmit: (token: string) => void;
};

export default function LoginPage({ onSubmit }: LoginPageProps) {
  const [token, setToken] = useState("");

  return (
    <div className="min-h-screen bg-background flex items-center justify-center p-6">
      <div className="w-full max-w-md rounded-xl border border-border bg-card p-6 space-y-5">
        <div className="space-y-1">
          <h1 className="text-xl font-semibold text-foreground">Admin Login</h1>
          <p className="text-sm text-muted-foreground">
            Enter admin API token to access the panel.
          </p>
        </div>

        <form
          className="space-y-3"
          onSubmit={(event) => {
            event.preventDefault();
            onSubmit(token);
          }}
        >
          <Input
            type="password"
            placeholder="ADMIN_API_TOKEN"
            value={token}
            onChange={(event) => setToken(event.target.value)}
            autoFocus
          />
          <Button type="submit" className="w-full" disabled={!token.trim()}>
            Sign in
          </Button>
        </form>
      </div>
    </div>
  );
}
