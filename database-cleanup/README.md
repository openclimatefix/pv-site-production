# database-cleanup service

This service is responsible to remove old forecasts from the database and archive them.

## Entry point

```bash
DB_URL=<your_db_url> poetry run python database_cleanup/app.py
```

## Development

Format the code base *in place*

    make format

Lint the code

    make lint

Run the tests

    make test
