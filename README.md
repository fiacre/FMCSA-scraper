# scraper
Scraping data from FMCSA 
Flakes and demo 

fmcsa.py defines a SQLAlchemy model
The modules in scraper/ define what fields are populated from the FMCSA page
scraper_property.py defines a data descriptor that allows modules in scraper/ to use an XPath expression to get data.
Each scraper module creates a report when all data is scraped and validated or raises an exception. 
