# FUT Traders Local Test Environment

## 1. Create a Local PostgreSQL Database
```bash
docker run --name futtrader-db \
-e POSTGRES_USER=postgres \
-e POSTGRES_PASSWORD=postgres \
-e POSTGRES_DB=futtrader \
-p 5432:5432 -d postgres
```
Then create the table:
```bash
psql -h localhost -U postgres -d futtrader -f sql/create_tables.sql
```

## 2. Install Dependencies
```bash
pip install -r requirements.txt
```

## 3. Run the Bot
```bash
python bot.py
```

## 4. Start FastAPI Backend
```bash
uvicorn backend.main:app --reload
```

## 5. Test API
- http://127.0.0.1:8000/api/players → Fetch dummy players.
- http://127.0.0.1:8000/api/price/101 → Fetch Haaland's data.

## 6. Integrate Dashboard Locally
Point your React frontend to:
```
http://127.0.0.1:8000/api
```
