# Salesforce Metadata Documentation Generator

This repository provides a tool for extracting Salesforce metadata, including custom fields and relationships, and exporting it to a DuckDB database and Excel files.

Future plan is for this to be a CLI tool that can be used to generate documentation for Salesforce orgs and their metadata then generate a erd diagram.

## Table of Contents

- [Features](#features)
- [Requirements](#requirements)
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
- [License](#license)

## Features

- Connect to Salesforce and retrieve metadata for standard and custom objects.
- Store metadata in a DuckDB database.
- Export metadata and relationships to Excel files.
- Verify relationships between objects.

## Requirements

- Python 3.8 or higher
- Required Python packages:
  - `simple-salesforce`
  - `duckdb`
  - `pandas`
  - `typer`
  - `python-dotenv`
  - `openpyxl`
- [UV](https://github.com/astral-sh/uv) for managing Python environments and dependencies.

## Installation

1. Clone the repository:

   ```bash
   git clone https://github.com/yourusername/salesforce-metadata-documentation.git
   cd salesforce-metadata-documentation
   ```

2. Install `uv` if you haven't already:

   ```bash
   pip install uv
   ```

3. Use `uv` to create a virtual environment and install dependencies:

   ```bash
   uv install
   ```

4. Create a `.env` file in the root directory of the project with the following content:

   ```plaintext
   SALESFORCE_USERNAME=your_salesforce_username
   SALESFORCE_PASSWORD=your_salesforce_password
   SALESFORCE_SECURITY_TOKEN=your_salesforce_security_token
   SALESFORCE_DOMAIN=login  # or 'test' for sandbox
   ```

5. Ensure you have access to the Salesforce objects and fields you want to extract.

## Usage

You can run the application using the command line. The main entry point is `main.py`.

### Refresh Metadata

To refresh the metadata from Salesforce and store it in DuckDB:

```bash
uv run main.py refresh
```

### Export to Excel

To export the metadata to an Excel file:

```bash
uv run main.py excel
```

### Run All Operations

To refresh the metadata and export it to Excel in one command:

```bash
uv run main.py all
```

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.