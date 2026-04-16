import { useEffect, useState } from "react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { HashRouter, Route, Routes, Navigate } from "react-router-dom";
import { Toaster as Sonner } from "@/components/ui/sonner";
import { Toaster } from "@/components/ui/toaster";
import { TooltipProvider } from "@/components/ui/tooltip";
import { getAdminApiToken, setAdminApiToken, subscribeAdminApiTokenChange } from "@/lib/auth";
import { api } from "@/lib/api";
import DashboardPage from "./pages/DashboardPage";
import UsersPage from "./pages/UsersPage";
import KeysPage from "./pages/KeysPage";
import SessionsPage from "./pages/SessionsPage";
import TasksPage from "./pages/TasksPage";
import AuditPage from "./pages/AuditPage";
import LoginPage from "./pages/LoginPage";
import NotFound from "./pages/NotFound";

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      retry: 2,
      retryDelay: (attempt) => Math.min(1000 * 2 ** attempt, 10_000),
      refetchOnWindowFocus: false,
    },
    mutations: {
      retry: 0,
    },
  },
});

const App = () => {
  const [isAuthenticated, setIsAuthenticated] = useState(() => Boolean(getAdminApiToken()));

  useEffect(() => {
    return subscribeAdminApiTokenChange(() => {
      setIsAuthenticated(Boolean(getAdminApiToken()));
      queryClient.clear();
    });
  }, []);

  return (
    <QueryClientProvider client={queryClient}>
      <TooltipProvider>
        <Toaster />
        <Sonner />
        <HashRouter>
          <Routes>
            {isAuthenticated ? (
              <>
                <Route path="/login" element={<Navigate to="/" replace />} />
                <Route path="/" element={<DashboardPage />} />
                <Route path="/users" element={<UsersPage />} />
                <Route path="/keys" element={<KeysPage />} />
                <Route path="/sessions" element={<SessionsPage />} />
                <Route path="/tasks" element={<TasksPage />} />
                <Route path="/audit" element={<AuditPage />} />
                <Route path="*" element={<NotFound />} />
              </>
            ) : (
              <>
                <Route
                  path="/login"
                  element={
                    <LoginPage
                      onSubmit={async (token) => {
                        await api.verifyToken(token);
                        setAdminApiToken(token);
                        setIsAuthenticated(true);
                      }}
                    />
                  }
                />
                <Route path="*" element={<Navigate to="/login" replace />} />
              </>
            )}
          </Routes>
        </HashRouter>
      </TooltipProvider>
    </QueryClientProvider>
  );
};

export default App;
