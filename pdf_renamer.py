import time
import logging
import re
import threading
import yaml
import requests
from pathlib import Path
from typing import Optional


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

        self.docling_enable_ocr = self.config['docling'].get('enable_ocr', True)
        self.docling_force_ocr = self.config['docling'].get('force_ocr', False)
        self.docling_image_export_mode = self.config['docling'].get('image_export_mode', 'none')
        self.docling_ocr_engine = self.config['docling'].get('ocr_engine', 'easyocr')

        self.ollama_host = self.config['ollama']['host']
        self.ollama_port = self.config['ollama']['port']
        self.ollama_model = self.config['ollama']['model']
        self.ollama_prompt = self.config['ollama']['prompt']
        
        self.retry_interval = self.config['retry']['interval_seconds']
        self.max_attempts = self.config['retry']['max_attempts']
        self.polling_interval = self.config.get('polling', {}).get('interval_seconds', 5)

        logging.basicConfig(
            level=getattr(logging, self.config['logging']['level']),
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)


class PDFProcessor:
    def __init__(self, config: Config):
        self.config = config
        self.logger = config.logger
        
    def extract_text_from_pdf(self, pdf_path: Path) -> Optional[str]:
        try:
            docling_url = f"http://{self.config.docling_host}:{self.config.docling_port}/v1/convert/file"

            with open(pdf_path, 'rb') as f:
                files = {'files': (pdf_path.name, f, 'application/pdf')}
                data = {
                    'from_formats': ["docx", "pptx", "html", "image", "pdf", "asciidoc", "md", "xlsx"],
                    'to_formats': [self.config.docling_format],
                    'do_ocr': self.config.docling_enable_ocr,
                    'force_ocr': self.config.docling_force_ocr,
                    'image_export_mode': self.config.docling_image_export_mode,
                    'ocr_engine': self.config.docling_ocr_engine
                }

                response = requests.post(docling_url, files=files, data=data, timeout=600)
                response.raise_for_status()

                result = response.json()

                doc = result.get('document', {})
                if self.config.docling_format == 'md':
                    return doc.get('md_content', '')
                elif self.config.docling_format == 'text':
                    return doc.get('text_content', '')
                else:
                    return doc.get('md_content', doc.get('text_content', ''))

        except requests.exceptions.HTTPError as e:
            self.logger.error(f"Fehler beim Extrahieren von Text aus {pdf_path}: {e} - Response: {e.response.text}")
            return None
        except Exception as e:
            self.logger.error(f"Fehler beim Extrahieren von Text aus {pdf_path}: {e}")
            return None
    
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
    
    def process_pdf(self, pdf_path: Path) -> bool:
        try:
            self.logger.info(f"Verarbeite PDF: {pdf_path.name}")
            
            text = self.extract_text_from_pdf(pdf_path)
            if not text:
                raise Exception("Kein Text extrahiert")
            
            summary = self.generate_summary(text)
            if not summary:
                raise Exception("Keine Zusammenfassung generiert")
            
            new_filename = f"{summary}.pdf"
            new_path = self.config.processed_dir / new_filename
            
            counter = 1
            while new_path.exists():
                new_filename = f"{summary}_{counter}.pdf"
                new_path = self.config.processed_dir / new_filename
                counter += 1
            
            pdf_path.rename(new_path)
            self.logger.info(f"PDF erfolgreich verarbeitet: {pdf_path.name} -> {new_filename}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Fehler beim Verarbeiten von {pdf_path.name}: {e}")
            
            if pdf_path.exists():
                try:
                    error_path = self.config.error_dir / pdf_path.name
                    counter = 1
                    while error_path.exists():
                        error_path = self.config.error_dir / f"{pdf_path.stem}_{counter}{pdf_path.suffix}"
                        counter += 1

                    pdf_path.rename(error_path)
                    self.logger.error(f"PDF in error verschoben: {pdf_path.name}")
                except OSError as rename_error:
                    self.logger.error(f"Konnte Datei nicht verschieben: {rename_error}")
            
            return False


def retry_error_files(processor: PDFProcessor):
    attempt_pattern = re.compile(r'_attempt(\d+)$')

    while True:
        time.sleep(processor.config.retry_interval)

        error_files = [f for f in processor.config.error_dir.iterdir()
                       if f.suffix.lower() == '.pdf']

        if error_files:
            processor.logger.info(f"Versuche {len(error_files)} PDFs aus error-Verzeichnis erneut zu verarbeiten")

            for pdf_path in error_files:
                try:
                    stem = pdf_path.stem
                    match = attempt_pattern.search(stem)

                    if match:
                        attempts = int(match.group(1))
                        base_stem = stem[:match.start()]
                    else:
                        attempts = 1
                        base_stem = stem

                    if attempts >= processor.config.max_attempts:
                        processor.logger.warning(f"Max. Versuche erreicht für {pdf_path.name}, wird nicht erneut verarbeitet")
                        continue

                    new_stem = f"{base_stem}_attempt{attempts + 1}"
                    watch_path = processor.config.watch_dir / f"{new_stem}{pdf_path.suffix}"

                    counter = 1
                    while watch_path.exists():
                        watch_path = processor.config.watch_dir / f"{new_stem}_{counter}{pdf_path.suffix}"
                        counter += 1

                    pdf_path.rename(watch_path)
                    processor.logger.info(f"PDF aus error zurück in watch verschoben: {pdf_path.name} -> {watch_path.name} (Versuch {attempts + 1})")

                except Exception as e:
                    processor.logger.error(f"Fehler beim Verschieben von {pdf_path.name}: {e}")


def poll_directory(processor: PDFProcessor):
    for f in processor.config.watch_dir.iterdir():
        if f.suffix.lower() == '.pdf':
            try:
                processor.process_pdf(f)
            except Exception as e:
                processor.logger.error(f"Fehler bei {f.name}: {e}")


def main():
    config = Config()

    if not config.watch_dir.exists():
        config.logger.error(f"Watch-Verzeichnis existiert nicht: {config.watch_dir}")
        return
    config.processed_dir.mkdir(parents=True, exist_ok=True)
    config.error_dir.mkdir(parents=True, exist_ok=True)

    processor = PDFProcessor(config)

    config.logger.info(f"PDF-Überwachung gestartet: {config.watch_dir} (Polling-Intervall: {config.polling_interval}s)")

    retry_thread = threading.Thread(target=retry_error_files, args=(processor,), daemon=True)
    retry_thread.start()

    try:
        while True:
            poll_directory(processor)
            time.sleep(config.polling_interval)
    except KeyboardInterrupt:
        config.logger.info("PDF-Überwachung beendet")
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