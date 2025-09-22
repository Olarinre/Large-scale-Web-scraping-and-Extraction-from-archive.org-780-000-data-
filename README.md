# 📚 Archive.org Indian Digital Library Scraper

A high-performance web scraping system designed to systematically archive and extract metadata from the Indian digital library collection on Archive.org.  
This project handles **massive-scale data processing** while maintaining server-friendly practices and ensuring **data integrity**.

## 🌟 Overview
This project addresses the challenge of processing Archive.org's **Indian digital library collection** at scale:

- **380,000+ unique identifiers** representing cultural, historical, and academic resources  
- **760,000+ individual pages** to download and process  
- **Intelligent resume functionality** for long-running operations  
- **Server-respectful scraping** with automatic rate limiting  

The system operates in **two phases**:
1. Downloading HTML content  
2. Extracting structured data  

---

## ✨ Features

### Core Capabilities
- **Massive Scale Processing** – Handles hundreds of thousands of records efficiently  
- **Parallel Execution** – Up to 40 concurrent workers for optimal performance  
- **Fault-Tolerant Design** – Automatic retry logic and error recovery  
- **Proxy Rotation** – Built-in support with automatic failover  
- **Resume Functionality** – Continues from interruption points without duplication  

### Technical Excellence
- **Structured Data Extraction** – Clean JSON output with preserved HTML structure  
- **Storage Management** – Automatic cleanup of temporary files  
- **Progress Monitoring** – Real-time reporting with detailed statistics  
- **Memory Efficient** – Streamed processing for large datasets  

---

## 🚀 Installation

### Prerequisites
- Python **3.8+**  
- **20+ GB free disk space** (for full dataset)  

### Dependencies
pip install requirements.txt

## System Requirements
- Storage: 20+ GB free space
- Memory: 8+ GB RAM recommended
- Network: Stable internet connection
- Python: 3.8+

## Legal Considerations
For educational, academic, and personal archival use only
Commercial use may require additional permissions from Archive.org

## 🤝 Contributing
Contributions are welcome! Please submit a Pull Request.
Areas for Improvement:

- Performance optimization
- Additional metadata extraction
- Enhanced error recovery
- Docker support
- Web-based monitoring dashboard
- Reporting Issues

## 📄 License
This project is intended for educational and research purposes.
Users are responsible for complying with Archive.org’s terms of service and applicable laws.



