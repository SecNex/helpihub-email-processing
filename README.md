# SecNex ITSM - Email Processing Engine

## Description

This is a Python script that processes incoming emails and updates the database accordingly.

## Requirements

- Python 3.10 or higher
- PostgreSQL 14 or higher
- IMAP server
- SMTP server
- Database with the following tables:
  - `emails`
  - `tickets`
  - `ticket_assignments`
  - `supporters`

## Installation

1. Clone the repository
2. Install the required dependencies
3. Configure the database connection in the `config.ini` file
4. Run the script

## Usage

1. Run the script
2. The script will process all new emails in the inbox and update the database accordingly

## License

This project is open-sourced under the MIT License - see the LICENSE file for details
