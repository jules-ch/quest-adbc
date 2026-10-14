The repository-root `docker-compose.yml` starts a QuestDB image for local integration tests:

```sh
docker compose up -d --wait
$env:QUESTDB_ADBC_TEST_URI = "ws://localhost:9000"
cargo test --test integration -- --nocapture
docker compose down -v
```
