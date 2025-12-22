import time
import logging
import re
import threading
import yaml
import requests
from pathlib import Path
from typing import Optional

DEFAULT_SUPPORTED_EXTENSIONS = {
    '.pdf': 'application/pdf',
    '.docx': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
    '.pptx': 'application/vnd.openxmlformats-officedocument.presentationml.presentation',
    '.xlsx': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    '.html': 'text/html',
    '.htm': 'text/html',
    '.md': 'text/markdown',
    '.adoc': 'text/asciidoc',
    '.asciidoc': 'text/asciidoc',
    '.png': 'image/png',
    '.jpg': 'image/jpeg',
    '.jpeg': 'image/jpeg',
    '.tiff': 'image/tiff',
    '.tif': 'image/tiff',
    '.bmp': 'image/bmp',
    '.gif': 'image/gif',
    '.webp': 'image/webp',
}


class Config:
    def __init__(self, config_path: str = "config.yaml"):
        with open(config_path, 'r', encoding='utf-8') as f:
            self.config = yaml.safe_load(f)

        self.watch_dir = Path(self.config['watch_directory'])
        self.processed_dir = self.watch_dir / "Verarbeitet"
        self.error_dir = self.watch_dir / "Fehler"

        self.docling_host = self.config['docling']['host']
        self.docling_port = self.config['docling']['port']
        self.docling_format = self.config['docling']['format']

        self.docling_image_export_mode = self.config['docling'].get('image_export_mode', 'none')
        self.docling_ocr_engine = self.config['docling'].get('ocr_engine', 'easyocr')

        self.ollama_host = self.config['ollama']['host']
        self.ollama_port = self.config['ollama']['port']
        self.ollama_model = self.config['ollama']['model']
        self.ollama_prompt = self.config['ollama']['prompt']

        self.retry_interval = self.config['retry']['interval_seconds']
        self.max_attempts = self.config['retry']['max_attempts']
        self.polling_interval = self.config.get('polling', {}).get('interval_seconds', 5)

        self._load_supported_extensions()

        logging.basicConfig(
            level=getattr(logging, self.config['logging']['level']),
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)

    def _load_supported_extensions(self):
        configured_extensions = self.config.get('supported_extensions')
        
        if configured_extensions is None:
            self.supported_extensions = DEFAULT_SUPPORTED_EXTENSIONS.copy()
        elif isinstance(configured_extensions, list):
            self.supported_extensions = {}
            for ext in configured_extensions:
                ext_lower = ext.lower() if ext.startswith('.') else f'.{ext.lower()}'
                if ext_lower in DEFAULT_SUPPORTED_EXTENSIONS:
                    self.supported_extensions[ext_lower] = DEFAULT_SUPPORTED_EXTENSIONS[ext_lower]
                else:
                    self.supported_extensions[ext_lower] = 'application/octet-stream'
        elif isinstance(configured_extensions, dict):
            self.supported_extensions = {
                (k.lower() if k.startswith('.') else f'.{k.lower()}'): v
                for k, v in configured_extensions.items()
            }
        else:
            self.supported_extensions = DEFAULT_SUPPORTED_EXTENSIONS.copy()

    def is_supported_file(self, file_path: Path) -> bool:
        return file_path.suffix.lower() in self.supported_extensions

    def get_mime_type(self, file_path: Path) -> str:
        return self.supported_extensions.get(file_path.suffix.lower(), 'application/octet-stream')


class DocumentProcessor:
    def __init__(self, config: Config):
        self.config = config
        self.logger = config.logger
        
    def extract_text_from_document(self, doc_path: Path) -> Optional[str]:
        try:
            docling_url = f"http://{self.config.docling_host}:{self.config.docling_port}/v1/convert/file"
            
            mime_type = self.config.get_mime_type(doc_path)
            
            force_ocr = doc_path.suffix.lower() == '.pdf' and doc_path.name.startswith("Xerox Scan_")

            text = self._call_docling(docling_url, doc_path, mime_type, force_ocr)
            
            if not text and not force_ocr:
                self.logger.info(f"Kein Text extrahiert, versuche erneut mit force_ocr=True: {doc_path.name}")
                text = self._call_docling(docling_url, doc_path, mime_type, force_ocr=True)
            
            return text

        except requests.exceptions.HTTPError as e:
            self.logger.error(f"Fehler beim Extrahieren von Text aus {doc_path}: {e} - Response: {e.response.text}")
            return None
        except Exception as e:
            self.logger.error(f"Fehler beim Extrahieren von Text aus {doc_path}: {e}")
            return None

    def _call_docling(self, docling_url: str, doc_path: Path, mime_type: str, force_ocr: bool) -> Optional[str]:
        with open(doc_path, 'rb') as f:
            files = {'files': (doc_path.name, f, mime_type)}
            data = {
                'from_formats': ["docx", "pptx", "html", "image", "pdf", "asciidoc", "md", "xlsx"],
                'to_formats': [self.config.docling_format],
                'do_ocr': True,
                'force_ocr': force_ocr,
                'image_export_mode': self.config.docling_image_export_mode,
                'ocr_engine': self.config.docling_ocr_engine
            }

            response = requests.post(docling_url, files=files, data=data, timeout=600)
            response.raise_for_status()

            result = response.json()

            doc = result.get('document', {})
            if self.config.docling_format == 'md':
                text = doc.get('md_content', '')
            elif self.config.docling_format == 'text':
                text = doc.get('text_content', '')
            else:
                text = doc.get('md_content', doc.get('text_content', ''))
            
            return text if text and text.strip() else None
    
    def generate_summary(self, text: str) -> Optional[str]:
        try:
            ollama_url = f"http://{self.config.ollama_host}:{self.config.ollama_port}/api/generate"
            
            payload = {
                "model": self.config.ollama_model,
                "prompt": f"{self.config.ollama_prompt}\n\n{text[:4000]}",
                "stream": False
            }
            
            response = requests.post(ollama_url, json=payload, timeout=600)
            response.raise_for_status()
            
            result = response.json()
            summary = result.get('response', '').strip()
            
            summary = re.sub(r'[\x00-\x1F\x7F]', '', summary)
            summary = summary.replace(' ', '_')
            summary = re.sub(r'[/\\?%*:|"<>]', '_', summary)
            summary = summary[:150]
            
            return summary if summary else None
            
        except requests.exceptions.HTTPError as e:
            self.logger.error(f"Fehler beim Generieren der Zusammenfassung: {e} - Response: {e.response.text}")
            return None
        except Exception as e:
            self.logger.error(f"Fehler beim Generieren der Zusammenfassung: {e}")
            return None
    
    def process_document(self, doc_path: Path) -> bool:
        try:
            self.logger.info(f"Verarbeite Dokument: {doc_path.name}")
            
            text = self.extract_text_from_document(doc_path)
            if not text:
                raise Exception("Kein Text extrahiert")
            
            summary = self.generate_summary(text)
            if not summary:
                raise Exception("Keine Zusammenfassung generiert")
            
            new_filename = f"{summary}{doc_path.suffix}"
            new_path = self.config.processed_dir / new_filename
            
            counter = 1
            while new_path.exists():
                new_filename = f"{summary}_{counter}{doc_path.suffix}"
                new_path = self.config.processed_dir / new_filename
                counter += 1
            
            doc_path.rename(new_path)
            self.logger.info(f"Dokument erfolgreich verarbeitet: {doc_path.name} -> {new_filename}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Fehler beim Verarbeiten von {doc_path.name}: {e}")
            
            if doc_path.exists():
                try:
                    error_path = self.config.error_dir / doc_path.name
                    counter = 1
                    while error_path.exists():
                        error_path = self.config.error_dir / f"{doc_path.stem}_{counter}{doc_path.suffix}"
                        counter += 1

                    doc_path.rename(error_path)
                    self.logger.error(f"Dokument in error verschoben: {doc_path.name}")
                except OSError as rename_error:
                    self.logger.error(f"Konnte Datei nicht verschieben: {rename_error}")
            
            return False


def retry_error_files(processor: DocumentProcessor):
    attempt_pattern = re.compile(r'_attempt(\d+)$')

    while True:
        time.sleep(processor.config.retry_interval)

        error_files = [f for f in processor.config.error_dir.iterdir()
                       if processor.config.is_supported_file(f)]

        if error_files:
            processor.logger.info(f"Versuche {len(error_files)} Dokumente aus error-Verzeichnis erneut zu verarbeiten")

            for doc_path in error_files:
                try:
                    stem = doc_path.stem
                    match = attempt_pattern.search(stem)

                    if match:
                        attempts = int(match.group(1))
                        base_stem = stem[:match.start()]
                    else:
                        attempts = 1
                        base_stem = stem

                    if attempts >= processor.config.max_attempts:
                        processor.logger.warning(f"Max. Versuche erreicht für {doc_path.name}, wird nicht erneut verarbeitet")
                        continue

                    new_stem = f"{base_stem}_attempt{attempts + 1}"
                    watch_path = processor.config.watch_dir / f"{new_stem}{doc_path.suffix}"

                    counter = 1
                    while watch_path.exists():
                        watch_path = processor.config.watch_dir / f"{new_stem}_{counter}{doc_path.suffix}"
                        counter += 1

                    doc_path.rename(watch_path)
                    processor.logger.info(f"Dokument aus error zurück in watch verschoben: {doc_path.name} -> {watch_path.name} (Versuch {attempts + 1})")

                except Exception as e:
                    processor.logger.error(f"Fehler beim Verschieben von {doc_path.name}: {e}")


def poll_directory(processor: DocumentProcessor):
    for f in processor.config.watch_dir.iterdir():
        if processor.config.is_supported_file(f):
            try:
                processor.process_document(f)
            except Exception as e:
                processor.logger.error(f"Fehler bei {f.name}: {e}")


def main():
    config = Config()

    if not config.watch_dir.exists():
        raise FileNotFoundError(f"Watch-Verzeichnis existiert nicht: {config.watch_dir}")
    config.processed_dir.mkdir(parents=True, exist_ok=True)
    config.error_dir.mkdir(parents=True, exist_ok=True)

    processor = DocumentProcessor(config)

    supported_ext_list = ', '.join(config.supported_extensions.keys())
    config.logger.info(f"Dokument-Überwachung gestartet: {config.watch_dir} (Polling-Intervall: {config.polling_interval}s)")
    config.logger.info(f"Unterstützte Dateitypen: {supported_ext_list}")

    retry_thread = threading.Thread(target=retry_error_files, args=(processor,), daemon=True)
    retry_thread.start()

    try:
        while True:
            poll_directory(processor)
            time.sleep(config.polling_interval)
    except KeyboardInterrupt:
        config.logger.info("Dokument-Überwachung beendet")
        raise


if __name__ == "__main__":
    while True:
        try:
            main()
        except KeyboardInterrupt:
            break
        except Exception as e:
            logging.error(f"Unerwarteter Fehler, Neustart in 10 Sekunden: {e}")
            time.sleep(10)