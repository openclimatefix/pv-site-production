# database-cleanup service

This service is responsible to remove old forecasts from the database and archive them.

Old forecast are saved to a directory defined by `SAVE_DIR` environment variable.
We current only save the forecast data, if the site is in a site group with service level 1 or above.
## Running the service

```bash
poetry install
DB_URL=<your_db_url> poetry run python database_cleanup/app.py
```

## Development

Format the code base *in place*

    make format

Lint the code

    make lint

Run the tests

    make test
