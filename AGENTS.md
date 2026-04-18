# AGENTS

## Project Goal

Build a Vercel-ready analytical web app for studying MOEX instrument pairs, their spreads, ranges, and seasonality.

## Stack

- Frontend: static SPA (`index.html`, `app.js`, `styles.css`) with Plotly in the browser
- Backend: Python FastAPI serverless app in `api/index.py`
- Database: PostgreSQL via `DATABASE_URL`
- Deployment: Vercel + GitHub

## Product Rules

- Spread formula is always `close1 - close2`
- Requested start date must be adjusted to `max(user_start, instrument1_start, instrument2_start)`
- Requested end date must be adjusted to `min(today, nearest_futures_expiration)`
- Before returning analytics, the backend must sync missing MOEX candles into the database for the effective range
- The UI must support both curated instrument selection and manual ticker input

## Engineering Rules

- Keep changes small and direct
- Prefer clear SQL and Python over unnecessary abstraction
- Store 10-minute candles with idempotent upserts
- Treat MOEX instrument metadata as dynamic and refresh it when a ticker is requested
- Do not hardcode secrets in the repository

## Important Files

- `api/index.py`: API, MOEX sync, analytics, DB access
- `index.html`: app shell
- `app.js`: UI and Plotly rendering
- `styles.css`: app styling
- `vercel.json`: API rewrite for Vercel
- `README.md`: setup and deployment instructions
