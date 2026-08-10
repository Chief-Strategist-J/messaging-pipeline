 Run 10K Request Load Test via k6 Container

 docker run --rm --add-host=host.docker.internal:host-gateway \
  -v $(pwd)/event-platform/loadtest:/scripts \
  grafana/k6:latest run \
  --summary-export=/scripts/k6-results-10k.json \
  /scripts/ingestion_10k_loadtest.ts

pytest event-platform/loadtest/test_ingestion_pipeline.py -k test_load_10k_requests -vs

docker exec -it event-platform-postgres-1 psql -U app -d app -c "SELECT count(*) FROM raw_events;"
