from email_processor import EmailProcessor
import time
import logging
import gc

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    processor = None
    while True:
        try:
            if not processor:
                processor = EmailProcessor()
            processor.process_emails()
            logger.info("E-Mail-Verarbeitung abgeschlossen")
        except ValueError as e:
            logger.error(f"Konfigurationsfehler: {str(e)}")
            if processor:
                del processor
                processor = None
            gc.collect()  # Explizite Garbage Collection
            time.sleep(300)  # 5 Minuten warten bei Konfigurationsfehlern
        except Exception as e:
            logger.error(f"Fehler bei der Verarbeitung: {str(e)}", exc_info=True)
            if processor:
                del processor
                processor = None
            gc.collect()  # Explizite Garbage Collection
            time.sleep(60)  # 1 Minute warten bei anderen Fehlern
        
        time.sleep(10)  # 10 Sekunden warten zwischen den Durchläufen

if __name__ == "__main__":
    main() 