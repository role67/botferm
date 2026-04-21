# Admin Webpanel

Environment variables:

```env
VITE_API_URL=https://your-python-service.onrender.com
```

Auth flow:
- Open `/login` and enter `ADMIN_API_TOKEN`.
- Token is stored in `sessionStorage` for the current browser session.
- Panel sends `Authorization: Bearer ...` for all API requests.
