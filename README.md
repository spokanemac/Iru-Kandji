# Iru-Kandji Scripts
Scripts for interacting with the Iru Kandji MDM API.

Let's make the jellyfish dance!

## Bulk Operations

### Load Tags

- `iru-load-tags.py` 
Iru Kandji tag bulk creator with duplicate checks, optional prefetch, retries, concurrency,
rate-limiting, and CSV reporting.

Env:
  `API_TOKEN=...` set using `export API_TOKEN="token_here"`

Examples:
  ```
  python iru-load-tags.py -f tags.txt --prefetch --concurrency 8 --qps 5 --report-path report.csv
  ```
