# Fixtures

Primary fixture used by this repository:

- File: `fixtures/NPPF_December_2023.docx`
- Source URL: `https://data.parliament.uk/DepositedPapers/Files/DEP2023-1029/NPPF_December_2023.docx`

## Automatic Download

```bash
make fixtures
```

`make fixtures` uses `curl` first, then `wget`, and only downloads when the file is missing.

## Manual Fallback

If automatic download is unavailable (offline/CI restrictions):

1. Open this URL in a browser:  
   `https://data.parliament.uk/DepositedPapers/Files/DEP2023-1029/NPPF_December_2023.docx`
2. Save the file as:  
   `fixtures/NPPF_December_2023.docx`
3. Re-run pipeline commands (`make extract`, `make e2e`, etc.).

## Optional Reproducibility Check

After download, record a local checksum for your run notes:

```bash
shasum -a 256 fixtures/NPPF_December_2023.docx
```
