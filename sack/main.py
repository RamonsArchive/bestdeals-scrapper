from sack.scrapper import FacebookMarketplaceScraper

if __name__ == "__main__":
    try:
        scraper = FacebookMarketplaceScraper(headless=False)  # Set to True for production
        scraper.login()
        
        # Example: Scrape listings in San Diego
        results = scraper.scrape_marketplace_listings(zip_code="92101", max_listings=20)
        
        # Save results
        scraper.save_to_csv(results)
        
        # Print summary
        print(f"\nScraped {len(results)} listings")
        print("Columns collected:", list(results.columns))
        
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        # Always close the browser
        if 'scraper' in locals():
            scraper.close()