# imoex-data_analysis

Analytical web app for researching MOEX instrument pair spreads, ranges, and seasonality.

## Stack

- Python FastAPI serverless backend
- Static frontend with Plotly charts
- PostgreSQL for candle storage
- Vercel for deployment

## Features

- Loads and updates MOEX 10-minute candles on demand
- Stores OHLCV history in PostgreSQL
- Supports curated instruments and manual ticker input
- Computes overlapping pair periods automatically
- Builds price, spread, percent spread, and seasonality views

## Environment

Create `.env` from `.env.example` and set `DATABASE_URL`.

Recommended production DB: managed PostgreSQL with connection pooling, for example Supabase or Neon.

## Local Run

1. Create a virtual environment
2. Install dependencies:

```bash
pip install -r requirements.txt
```

3. Start the API locally:

```bash
uvicorn api.index:app --reload
```

4. Open `index.html` with a simple static server, for example:

```bash
python -m http.server 3000
```

Then open `http://localhost:3000`.

## Deploy

1. Push the repository to GitHub
2. Import the repo into Vercel
3. Add `DATABASE_URL` in Vercel project settings
4. Deploy

## Notes

- The backend creates required tables automatically
- Vercel serves the frontend statically and rewrites `/api/*` to the Python FastAPI app
