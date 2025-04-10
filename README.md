# ANEEL MMDG Photovoltaic Cleaner

![ANEEL Logo](<aneel_logo.png>)  
*Cleaning, merging and processing Brazil's official registry of mini/micro distributed solar generation (MMDG) ventures.*

## Features
- ⚡ **Automatic Data Fetching**: Downloads latest ANEEL datasets via API
- 🧹 **Data Validation**: Checks for missing CEG codes, IBGE codes, coordinates and malformed ones 
- 🔗 **Dataset Merging**: Unifies administrative and technical records
- ✂️ **Large File Handling**: Splits outputs into 800k-line chunks
- 📊 **Error Isolation**: Separates problematic records for review


## 📜 Critical Usage Notice
- For proper execution of all scripts, you must follow all [`main()`](src/unify_files.py#L119-L185) steps and read the comments thoughtfully.

## Data Sources
- Download Links: See [DATA_SOURCES.md](DATA_SOURCES.md) for direct URLs and version history.
- License: CC-BY 4.0 (Attribution required).
