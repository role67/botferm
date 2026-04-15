const TOKEN_STORAGE_KEY = "botoferma_admin_api_token";
const TOKEN_CHANGED_EVENT = "botoferma:admin-token-changed";

export function getAdminApiToken(): string {
  if (typeof window === "undefined") {
    return "";
  }
  return sessionStorage.getItem(TOKEN_STORAGE_KEY)?.trim() ?? "";
}

export function setAdminApiToken(token: string): void {
  if (typeof window === "undefined") {
    return;
  }
  const normalized = token.trim();
  if (normalized) {
    sessionStorage.setItem(TOKEN_STORAGE_KEY, normalized);
  } else {
    sessionStorage.removeItem(TOKEN_STORAGE_KEY);
  }
  window.dispatchEvent(new Event(TOKEN_CHANGED_EVENT));
}

export function clearAdminApiToken(): void {
  if (typeof window === "undefined") {
    return;
  }
  sessionStorage.removeItem(TOKEN_STORAGE_KEY);
  window.dispatchEvent(new Event(TOKEN_CHANGED_EVENT));
}

export function subscribeAdminApiTokenChange(listener: () => void): () => void {
  if (typeof window === "undefined") {
    return () => undefined;
  }
  window.addEventListener(TOKEN_CHANGED_EVENT, listener);
  return () => {
    window.removeEventListener(TOKEN_CHANGED_EVENT, listener);
  };
}
