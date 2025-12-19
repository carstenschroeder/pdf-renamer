# PDF Renamer

Automatic PDF renaming using Docling and Ollama.

## Features

- Watches a directory for new PDFs
- Extracts text using Docling
- Generates summary filename using Ollama
- Moves processed files to `processed/`, failed files to `error/`
- Retries failed files periodically

## Requirements

- Docker & Docker Compose
- Docling Server
- Ollama

## Usage

1. Configure `config.yaml` (watch directory, Docling/Ollama endpoints)
2. Start: `docker-compose up -d`
3. Drop PDFs into watch directory
4. View logs: `docker-compose logs -f pdf-renamer`
5. Stop: `docker-compose down`