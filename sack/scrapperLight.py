import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, StaleElementReferenceException
import undetected_chromedriver as uc
from webdriver_manager.chrome import ChromeDriverManager
from dotenv import load_dotenv
import os
import mysql.connector
from sqlalchemy import create_engine
from datetime import datetime

# Load environment variables
load_dotenv()

class FacebookMarketplaceScraper:
    def __init__(self, headless=False, use_undetected=True):
        """
        Initialize the Facebook Marketplace scraper
        
        Args:
            headless (bool): Run in headless mode (no browser UI)
            use_undetected (bool): Use undetected_chromedriver to bypass detection
        """
        # Save configuration
        self.headless = headless
        self.use_undetected = use_undetected
        
        # Initialize the driver
        self.setup_driver()
        
        # Keep track of login state
        self.is_logged_in = False
        
        # Database connection
        self.db_connection = None
        
    def setup_driver(self):
        """Set up and configure the WebDriver"""
        # Set up Chrome options
        chrome_options = Options() if not self.use_undetected else uc.ChromeOptions()
        
        if self.headless:
            chrome_options.add_argument("--headless=new")
            
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-notifications")
        chrome_options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
        chrome_options.add_argument("--enable-cookies")
        
        # Initialize the WebDriver
        if self.use_undetected:
            self.driver = uc.Chrome(options=chrome_options)
        else:
            self.driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
        
        # Set page load timeout and window size
        self.driver.set_page_load_timeout(20)
        self.driver.set_window_size(1920, 1080)
        
    def connect_to_db(self, host="localhost", user="root", password="sql_Ohuy228", database="daya_scooters"):
        """Connect to MySQL database"""
        try:
            self.db_connection = mysql.connector.connect(
                host=host,
                user=user,
                password=password,
                database=database
            )
            
            # Create sqlalchemy engine
            self.engine = create_engine(f"mysql+mysqlconnector://{user}:{password}@{host}/{database}")
            
            # Create table if not exists
            cursor = self.db_connection.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS marketplace_listings (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    title VARCHAR(255),
                    price FLOAT,
                    price_string VARCHAR(50),
                    location VARCHAR(255),
                    image_url TEXT,
                    url TEXT,
                    scraped_at DATETIME
                )
            """)
            self.db_connection.commit()
            
            print("Successfully connected to database")
            return True
        except Exception as e:
            print(f"Database connection error: {e}")
            return False
    
    def login(self):
        """Login to Facebook"""
        # Define your personal login details
        email = 'molokic228@hotmail.com'
        password = 'Facebook_huy228'
        
        # Go to Facebook login page
        self.driver.get("https://www.facebook.com/")
        
        try:
            # Handle cookie consent if it appears
            try:
                cookie_buttons = WebDriverWait(self.driver, 5).until(
                    EC.presence_of_all_elements_located((By.XPATH, 
                    "//button[contains(text(), 'Accept') or contains(text(), 'Allow') or contains(text(), 'Cookies') or contains(text(), 'Accept All')]"))
                )
                if cookie_buttons:
                    cookie_buttons[0].click()
                    time.sleep(1)
            except TimeoutException:
                print("No cookie consent needed or timed out waiting for it")
                
            # Close popup if it appears
            try:
                close_button = WebDriverWait(self.driver, 5).until(
                    EC.element_to_be_clickable((By.XPATH, "//div[contains(@aria-label, 'Close')]"))
                )
                close_button.click()
                time.sleep(1)
            except:
                pass
            
            # Check if already logged in
            if "log in" in self.driver.title.lower() or "sign in" in self.driver.title.lower() or "login" in self.driver.current_url:
                # Wait for email field
                email_field = WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.ID, "email"))
                )
                
                email_field.clear()
                email_field.send_keys(email)
                
                password_field = self.driver.find_element(By.ID, "pass")
                password_field.clear()
                password_field.send_keys(password)
                
                # Click login button
                login_button = WebDriverWait(self.driver, 5).until(
                    EC.element_to_be_clickable((By.NAME, "login"))
                )
                login_button.click()
                
                # Wait for login to complete
                try:
                    WebDriverWait(self.driver, 15).until(
                        EC.any_of(
                            EC.presence_of_element_located((By.XPATH, "//div[@aria-label='Facebook' or @aria-label='Home']")),
                            EC.presence_of_element_located((By.XPATH, "//a[contains(@href, '/me/')]")),
                            EC.presence_of_element_located((By.XPATH, "//div[@role='navigation']")),
                            EC.url_contains("facebook.com")
                        )
                    )
                    print("Login successful")
                    self.is_logged_in = True
                except TimeoutException:
                    print("Timeout waiting for login elements")
                
                # Handle post-login prompts
                try:
                    dont_save = WebDriverWait(self.driver, 5).until(
                        EC.element_to_be_clickable((By.XPATH, 
                        "//button[contains(text(), 'Not Now') or contains(text(), 'Cancel') or contains(text(), 'Skip')]"))
                    )
                    dont_save.click()
                except TimeoutException:
                    pass
            else:
                print("Already logged in")
                self.is_logged_in = True
            
        except Exception as e:
            print(f"Login failed: {e}")
    
    def extract_link_from_element(self, element):
        """Extract link from an element's HTML"""
        try:
            html = element.get_attribute("innerHTML")
            if html:
                split_segment = html.split('"')
                for i, segment in enumerate(split_segment):
                    if "/marketplace/item/" in segment or "/marketplace/groups" in segment:
                        return "https://www.facebook.com" + segment
                
                # Fallback: try to find links via a tag
                a_tags = element.find_elements(By.TAG_NAME, "a")
                for a in a_tags:
                    href = a.get_attribute("href")
                    if href and "/marketplace/item/" in href:
                        return href
        except Exception as e:
            print(f"Error extracting link: {e}")
        return None
    
    def extract_text_from_element(self, element, xpath):
        """Extract text from an element safely with proper error handling"""
        try:
            elements = element.find_elements(By.XPATH, xpath)
            if elements and elements[0].text:
                return elements[0].text.strip()
        except (NoSuchElementException, StaleElementReferenceException):
            pass
        return "N/A"
            
    def extract_attribute_from_element(self, element, xpath, attribute):
        """Extract attribute from an element safely with proper error handling"""
        try:
            elements = element.find_elements(By.XPATH, xpath)
            if elements:
                return elements[0].get_attribute(attribute)
        except (NoSuchElementException, StaleElementReferenceException):
            pass
        return "N/A"
    
    def url_modifier(self, item, city):
        """Format search URL"""
        item = item.replace(' ', '%20')
        return f'https://www.facebook.com/marketplace/{city}/search?query={item}&exact=false'
    
    def scrape_from_search(self, item, city, max_listings=50, scroll_count=5):
        """
        Scrape Facebook Marketplace listings by item and city
        
        Args:
            item (str): Item to search for
            city (str): City to search in
            max_listings (int): Maximum number of listings to scrape
            scroll_count (int): Number of times to scroll down to load more results
        """
        if not self.is_logged_in:
            print("Not logged in. Attempting to login...")
            self.login()
        
        # Navigate to the search URL
        search_url = self.url_modifier(item, city)
        try:
            print(f"Navigating to search URL: {search_url}")
            self.driver.get(search_url)
            
            # Wait for the page to load
            time.sleep(5)
            
        except Exception as e:
            print(f"Error navigating to search URL: {e}")
            return pd.DataFrame()
        
        listings_data = []
        
        try:
            # Define possible selectors for listing containers
            container_selectors = [
                "x3ct3a4",  # Common class for marketplace items
                "x6s0dn4",  # Alternative class
                "kbiprv82",  # Alternative class
                "x78zum5",   # Another possible class
            ]
            
            # Scroll to load more results
            for i in range(scroll_count):
                print(f"Scrolling ({i+1}/{scroll_count})...")
                
                # Scroll to bottom
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(2)  # Wait for content to load
                
                # For every third scroll, also scroll up a bit and back down to trigger lazy loading
                if i % 3 == 2:
                    self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight * 0.8);")
                    time.sleep(1)
                    self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                    time.sleep(2)
            
            # Try finding listings with different selectors
            listing_containers = []
            for selector in container_selectors:
                containers = self.driver.find_elements(By.CLASS_NAME, selector)
                if containers:
                    print(f"Found {len(containers)} listings using selector '{selector}'")
                    listing_containers = containers
                    break
            
            # If class-based selection fails, try XPath
            if not listing_containers:
                xpaths_to_try = [
                    "//div[contains(@data-testid, 'marketplace_search_feed')]//div[.//a[contains(@href, '/marketplace/item/')]]",
                    "//a[contains(@href, '/marketplace/item/')]",
                    "//div[contains(@class, 'x1xfsgkm')]//a"
                ]
                
                for xpath in xpaths_to_try:
                    containers = self.driver.find_elements(By.XPATH, xpath)
                    if containers:
                        print(f"Found {len(containers)} listings using XPath")
                        listing_containers = containers
                        break
            
            # Process each listing
            count = 0
            for listing in listing_containers:
                if count >= max_listings:
                    break
                    
                try:
                    # Extract listing URL first
                    listing_url = self.extract_link_from_element(listing)
                    if not listing_url:
                        continue
                    
                    # Parse the listing text
                    listing_text = listing.text.strip()
                    parts = listing_text.split('\n')
                    
                    # Initialize data dictionary
                    listing_data = {
                        "url": listing_url,
                        "scraped_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    }
                    
                    # Get image URL
                    img_src = self.extract_attribute_from_element(listing, ".//img", "src")
                    listing_data["image_url"] = img_src
                    
                    # Extract title, price, and location from text parts
                    if len(parts) >= 1:
                        # First part is usually price
                        price_str = parts[0]
                        listing_data["price_string"] = price_str
                        
                        # Clean up price value - extract just the number
                        if price_str.lower() == 'free':
                            listing_data["price"] = 0
                        else:
                            price_num = ''.join(filter(lambda x: x.isdigit() or x == '.', price_str))
                            try:
                                listing_data["price"] = float(price_num) if price_num else None
                            except ValueError:
                                listing_data["price"] = None
                    
                    if len(parts) >= 2:
                        # Second part is usually title/description
                        listing_data["title"] = parts[1]
                    
                    if len(parts) >= 3:
                        # Third part is usually location
                        listing_data["location"] = parts[2]
                    
                    # Add to our results
                    listings_data.append(listing_data)
                    print(f"Processed listing: {listing_data.get('title', 'Unknown')}")
                    count += 1
                    
                except Exception as e:
                    print(f"Error processing a listing: {e}")
            
        except Exception as e:
            print(f"Error scraping listings: {e}")
        
        # Convert to DataFrame
        df = pd.DataFrame(listings_data)
        
        # Ensure all important columns are present
        required_columns = ["title", "price", "price_string", "location", "image_url", "url", "scraped_at"]
        
        for column in required_columns:
            if column not in df.columns:
                df[column] = "N/A"
        
        return df
    
    def write_to_database(self, df, table_name="marketplace_listings"):
        """Write the scraped data to MySQL database"""
        if df.empty:
            print("No data to write to database")
            return False
            
        try:
            # Use SQLAlchemy for easy DataFrame writing
            df.to_sql(table_name, con=self.engine, if_exists='append', index=False)
            print(f"Successfully saved {len(df)} records to database table '{table_name}'")
            return True
        except Exception as e:
            print(f"Database write error: {e}")
            return False
    
    def save_to_csv(self, df, filename=None):
        """Save the DataFrame to a CSV file"""
        if df.empty:
            print("No data to save to CSV")
            return False
            
        if filename is None:
            # Generate filename with timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"facebook_marketplace_{timestamp}.csv"
            
        try:
            df.to_csv(filename, index=False, encoding='utf-8')
            print(f"Data saved to {filename}")
            return True
        except Exception as e:
            print(f"Error saving to CSV: {e}")
            return False
    
    def close(self):
        """Close connections"""
        try:
            self.driver.quit()
        except:
            pass
            
        if self.db_connection:
            try:
                self.db_connection.close()
            except:
                pass


# Example usage
if __name__ == "__main__":
    # Setup and configuration
    scraper = FacebookMarketplaceScraper(headless=False, use_undetected=True)
    
    try:
        # Connect to database
        scraper.connect_to_db()
        
        # Login to Facebook
        scraper.login()
        
        # Define search parameters
        search_item = "desk"  # Item to search for
        search_city = "seattle"  # City to search in
        
        # Scrape the data - increase scroll_count for more results
        results = scraper.scrape_from_search(
            item=search_item, 
            city=search_city, 
            max_listings=50,
            scroll_count=8  # Scroll more times to get more listings
        )
        
        # Save results
        if not results.empty:
            # Save to database
            scraper.write_to_database(results)
            
            # Save to CSV
            filename = f"{search_city}_{search_item}_{datetime.now().strftime('%Y%m%d')}.csv"
            scraper.save_to_csv(results, filename)
        else:
            print("No results found")
    
    except Exception as e:
        print(f"An error occurred during scraping: {e}")
    
    finally:
        # Close all connections
        scraper.close()