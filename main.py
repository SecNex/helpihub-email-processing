from email_processor import EmailProcessor
import time
import logging
import gc
import sys
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def main():
    processor = None
    logger.info("Starting email processing...")
    while True:
        try:
            if not processor:
                processor = EmailProcessor()
            processor.process_emails()
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
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt detected, exiting...")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}", exc_info=True)
        sys.exit(1)
