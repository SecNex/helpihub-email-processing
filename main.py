from email_processor import EmailProcessor
import time
import logging
import gc

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def main():
    processor = None
    while True:
        try:
            if not processor:
                processor = EmailProcessor()
            processor.process_emails()
            logger.info("Email processing completed")
        except ValueError as e:
            logger.error(f"Configuration error: {str(e)}")
            if processor:
                del processor
                processor = None
            gc.collect()  # Explicit garbage collection
            time.sleep(300)  # Wait 5 minutes on configuration errors
        except Exception as e:
            logger.error(f"Processing error: {str(e)}", exc_info=True)
            if processor:
                del processor
                processor = None
            gc.collect()  # Explicit garbage collection
            time.sleep(60)  # Wait 1 minute on other errors
        
        time.sleep(10)  # Wait 10 seconds between cycles

if __name__ == "__main__":
    main() 