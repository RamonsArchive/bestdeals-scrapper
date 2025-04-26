import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, StaleElementReferenceException
from webdriver_manager.chrome import ChromeDriverManager
from dotenv import load_dotenv
import os
import re

# Load environment variables
load_dotenv()

class FacebookMarketplaceScraper:
    def __init__(self, headless=False):
        # Set up Chrome options
        chrome_options = Options()
        if headless:
            chrome_options.add_argument("--headless=new")  # Updated headless argument
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-notifications")  # Disable notifications
        
        # Add user-agent to appear more like a regular browser
        chrome_options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
        
        # Enable cookies
        chrome_options.add_argument("--enable-cookies")
        
        # Initialize the WebDriver
        self.driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
        
        # Set page load timeout
        self.driver.set_page_load_timeout(30)
        
        # Set window size - larger size to see more elements
        self.driver.set_window_size(1920, 1080)
        
        # Keep track of login state
        self.is_logged_in = False
        
    def login(self):
        """Login to Facebook by directly navigating to Marketplace which will prompt login"""
        email = os.getenv('FACEBOOK_EMAIL')
        password = os.getenv('FACEBOOK_PASSWORD')
        
        if not email or not password:
            raise ValueError("Facebook credentials not found in environment variables")
        
        # Go directly to Facebook Marketplace which will redirect to login if needed
        self.driver.get("https://www.facebook.com/marketplace/")
        
        try:
            # Handle cookie consent if it appears
            try:
                cookie_buttons = WebDriverWait(self.driver, 5).until(
                    EC.presence_of_all_elements_located((By.XPATH, 
                    "//button[contains(text(), 'Accept') or contains(text(), 'Allow') or contains(text(), 'Cookies') or contains(text(), 'Accept All')]"))
                )
                if cookie_buttons:
                    cookie_buttons[0].click()
                    time.sleep(2)
            except TimeoutException:
                print("No cookie consent needed or timed out waiting for it")
            
            # Print page title to help debug
            print(f"Current page title: {self.driver.title}")
            
            # Check if we need to login
            if "log in" in self.driver.title.lower() or "sign in" in self.driver.title.lower() or "login" in self.driver.current_url:
                print("Login page detected, proceeding with authentication")
                
                # Wait for email field to appear with extended timeout
                email_field = WebDriverWait(self.driver, 15).until(
                    EC.presence_of_element_located((By.ID, "email"))
                )
                
                # Clear fields and enter credentials
                email_field.clear()
                email_field.send_keys(email)
                print(f"Entered email: {email[:3]}***{email[-3:]}")
                
                password_field = self.driver.find_element(By.ID, "pass")
                password_field.clear()
                password_field.send_keys(password)
                print("Entered password")
                
                # Click login button
                login_button = WebDriverWait(self.driver, 5).until(
                    EC.element_to_be_clickable((By.NAME, "login"))
                )
                login_button.click()
                print("Clicked login button")
                
                # Wait for login to complete with extended timeout
                try:
                    WebDriverWait(self.driver, 20).until(
                        EC.any_of(
                            EC.presence_of_element_located((By.XPATH, "//div[@aria-label='Facebook' or @aria-label='Home']")),
                            EC.presence_of_element_located((By.XPATH, "//a[contains(@href, '/me/')]")),
                            EC.presence_of_element_located((By.XPATH, "//div[@role='navigation']")),
                            EC.presence_of_element_located((By.XPATH, "//div[contains(@class, 'x1n2onr6')]")),
                            EC.url_contains("facebook.com/marketplace")
                        )
                    )
                    print("Login elements detected")
                except TimeoutException:
                    print("Timeout waiting for login elements, continuing...")
                    
                    # Try to continue anyway - Facebook UI might have changed
                    if "facebook.com" in self.driver.current_url and "/login" not in self.driver.current_url:
                        print("URL indicates we might be logged in despite timeout")
                
                # Check for common post-login scenarios
                # Scenario 1: "Save login info?" prompt
                try:
                    dont_save = WebDriverWait(self.driver, 5).until(
                        EC.element_to_be_clickable((By.XPATH, 
                        "//button[contains(text(), 'Not Now') or contains(text(), 'Cancel') or contains(text(), 'Skip')]"))
                    )
                    dont_save.click()
                    print("Clicked 'Not Now' on save login prompt")
                    time.sleep(2)
                except TimeoutException:
                    print("No 'Save login info' prompt or it was already handled")
                
                # Scenario 3: Security checkpoint/verification
                if "checkpoint" in self.driver.current_url:
                    print("WARNING: Security checkpoint detected")
                    print("Please complete the verification manually within 60 seconds")
                    time.sleep(60)  # Give time for manual intervention
                
                # Scenario 4: 2FA if enabled
                if "twofactor" in self.driver.current_url or "2fac" in self.driver.current_url:
                    print("WARNING: Two-factor authentication required")
                    print("Please complete 2FA manually within 60 seconds")
                    time.sleep(60)  # Give time for manual intervention
            else:
                print("Already appears to be logged in, no login form detected")
            
            # Check current URL to confirm login status
            print(f"Current URL after login attempt: {self.driver.current_url}")
            
            # Consider ourselves logged in if we're on marketplace or not on the login page
            if "/login" not in self.driver.current_url:
                self.is_logged_in = True
                print("Login successful based on URL change")
            else:
                print("WARNING: Still appears to be on login page")
            
            # Ensure we are on Marketplace page
            if "/marketplace" not in self.driver.current_url:
                print("Navigating to Marketplace...")
                self.driver.get("https://www.facebook.com/marketplace/")
                time.sleep(5)
            
            # Wait for Marketplace to load
            try:
                WebDriverWait(self.driver, 15).until(
                    EC.any_of(
                        EC.presence_of_element_located((By.XPATH, "//span[contains(text(), 'Marketplace')]")),
                        EC.presence_of_element_located((By.XPATH, "//a[contains(@href, '/marketplace/')]")),
                        EC.presence_of_element_located((By.XPATH, "//h1[contains(text(), 'Marketplace')]"))
                    )
                )
                print("Marketplace elements detected")
                self.is_logged_in = True
            except TimeoutException:
                print("Timed out waiting for Marketplace elements")
                
            if self.is_logged_in:
                print("Successfully logged in and accessed Facebook Marketplace")
            else:
                print("Login might have succeeded but couldn't confirm Marketplace access")
            
        except Exception as e:
            print(f"Login failed: {e}")
            
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
    
    def scrape_marketplace_listings(self, zip_code=None, category=None, max_listings=10):
        """
        Scrape Facebook Marketplace listings with improved data extraction
        """
        if not self.is_logged_in:
            print("Not logged in. Attempting to login...")
            self.login()
        
        # Navigate to marketplace if not already there
        if "/marketplace" not in self.driver.current_url:
            self.driver.get("https://www.facebook.com/marketplace/")
            time.sleep(5)
        
        # Apply zip code filter if provided
        if zip_code:
            try:
                # Look for filter/location buttons - there are different possible UI states
                filter_buttons = self.driver.find_elements(By.XPATH, 
                    "//span[contains(text(), 'Location') or contains(text(), 'Filters')]")
                
                if filter_buttons:
                    filter_buttons[0].click()
                    time.sleep(2)
                    
                    # Try to find zip code input field
                    zip_inputs = self.driver.find_elements(By.XPATH, 
                        "//input[@placeholder='ZIP code' or @placeholder='City, state or zip']")
                    
                    if zip_inputs:
                        zip_inputs[0].clear()
                        zip_inputs[0].send_keys(zip_code)
                        time.sleep(1)
                        
                        # Look for apply/update/search button
                        apply_buttons = self.driver.find_elements(By.XPATH, 
                            "//span[contains(text(), 'Apply') or contains(text(), 'Update') or contains(text(), 'Search')]")
                        
                        if apply_buttons:
                            apply_buttons[0].click()
                            time.sleep(3)
                        else:
                            print("Could not find apply button for location filter")
                    else:
                        print("Could not find zip code input field")
                else:
                    print("Could not find location filter button")
                    
            except Exception as e:
                print(f"Error applying location filter: {e}")
        
        # Get all listings on the page
        listings_data = []
        
        try:
            # Wait for listings to load - check multiple possible container classes
            listing_containers = []
            xpaths_to_try = [
                "//div[contains(@class, 'x3ct3a4')]",
                "//div[contains(@class, 'kbiprv82')]",
                "//div[contains(@data-testid, 'marketplace_home_feed')]//div[a]",
                "//a[.//img and .//span[contains(@class, 'x1lliihq')]]"  # Based on provided HTML
            ]
            
            for xpath in xpaths_to_try:
                try:
                    WebDriverWait(self.driver, 10).until(
                        EC.presence_of_element_located((By.XPATH, xpath))
                    )
                    listing_containers = self.driver.find_elements(By.XPATH, xpath)
                    if listing_containers:
                        print(f"Found {len(listing_containers)} listings using {xpath}")
                        break
                except:
                    continue
            
            # Scroll to load more results
            for _ in range(3):
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(2)
            
            # Process each listing
            count = 0
            for listing in listing_containers:
                if count >= max_listings:
                    break
                    
                try:
                    # Extract basic data from listing preview
                    listing_data = {}
                    
                    # Get listing URL first (we need this even if other data fails)
                    try:
                        # First try to find the link within the listing
                        a_tags = listing.find_elements(By.TAG_NAME, "a")
                        if a_tags:
                            listing_url = a_tags[0].get_attribute("href")
                        else:
                            # If the listing itself is an <a> tag
                            listing_url = listing.get_attribute("href")
                            
                        if listing_url:
                            listing_data["url"] = listing_url
                        else:
                            # Skip items without URLs
                            continue
                    except Exception as e:
                        print(f"Could not get URL, skipping listing: {e}")
                        continue
                    
                    # Get title - based on the HTML structure provided
                    title = self.extract_text_from_element(listing, ".//span[contains(@class, 'x1lliihq x6ikm8r x10wlt62 x1n2onr6')]")
                    if title != "N/A":
                        listing_data["title"] = title
                    else:
                        # Fallback to any title-like span
                        title = self.extract_text_from_element(listing, ".//span[contains(@class, 'x1lliihq')]")
                        listing_data["title"] = title
                    
                    # Get price - based on the HTML structure provided
                    price = self.extract_text_from_element(listing, ".//span[contains(@class, 'x193iq5w') and contains(@class, 'xlh3980')]")
                    if price != "N/A":
                        listing_data["price"] = price
                    else:
                        # Fallback to any span with $ sign
                        price_spans = listing.find_elements(By.XPATH, ".//span[contains(text(), '$')]")
                        if price_spans:
                            listing_data["price"] = price_spans[0].text
                        else:
                            listing_data["price"] = "N/A"
                    
                    # Get location - based on the HTML structure provided
                    location = self.extract_text_from_element(listing, ".//span[contains(@class, 'x1lliihq x6ikm8r x10wlt62 x1n2onr6 xlyipyv')]")
                    if location != "N/A":
                        listing_data["location"] = location
                    else:
                        # Try alternative location classes
                        location = self.extract_text_from_element(listing, ".//span[contains(@class, 'x1n2onr6')]")
                        listing_data["location"] = location
                    
                    # Get main image URL
                    img_src = self.extract_attribute_from_element(listing, ".//img", "src")
                    listing_data["image_url"] = img_src
                    
                    # Time listed - UPDATED based on new HTML structure
                    # Using the new abbr tag with aria-label
                    listed_time = self.extract_attribute_from_element(listing, ".//abbr", "aria-label")
                    
                    # If aria-label not available, try the inner span
                    if listed_time == "N/A":
                        listed_time = self.extract_text_from_element(listing, ".//abbr/span")
                    
                    # Multiple fallbacks for listed time
                    if listed_time == "N/A":
                        # Try alternative class paths
                        time_xpaths = [
                            ".//span[contains(text(), 'day') or contains(text(), 'hour') or contains(text(), 'min') or contains(text(), 'week')]",
                            ".//span[contains(@class, 'x1n2onr6')]//span[contains(text(), 'ago')]",
                            ".//span[contains(@class, 'x1s688f')]"  # Sometimes time is in this class
                        ]
                        
                        for xpath in time_xpaths:
                            time_elements = listing.find_elements(By.XPATH, xpath)
                            for element in time_elements:
                                text = element.text
                                if text and ("ago" in text.lower() or "day" in text.lower() or 
                                           "hour" in text.lower() or "week" in text.lower()):
                                    listed_time = text
                                    break
                            if listed_time != "N/A":
                                break
                            
                    listing_data["listed_time"] = listed_time
                    
                    # Check if we need to get detailed info
                    if listing_data.get("url"):
                        detailed_info = self.get_listing_details(listing_data["url"])
                        listing_data.update(detailed_info)
                    
                    listings_data.append(listing_data)
                    print(f"Processed listing: {listing_data.get('title', 'Unknown')}")
                    count += 1
                    
                except Exception as e:
                    print(f"Error processing a listing: {e}")
            
        except Exception as e:
            print(f"Error scraping listings: {e}")
        
        # Convert to DataFrame with improved columns
        df = pd.DataFrame(listings_data)
        
        # Ensure all important columns are present
        required_columns = [
            "title", "price", "location", "condition", "listed_time", 
            "description", "seller_name", "seller_joined_date", 
            "image_url", "map_location_image", "url"
        ]
        
        for column in required_columns:
            if column not in df.columns:
                df[column] = "N/A"
        
        return df
    
    def get_listing_details(self, listing_url):
        """
        Get additional details from a listing's dedicated page with improved selectors
        based on the example data you provided
        """
        # Open a new tab to avoid losing the main results page
        self.driver.execute_script(f"window.open('{listing_url}');")
        self.driver.switch_to.window(self.driver.window_handles[1])
        
        # Wait for page to load
        time.sleep(5)
        
        detailed_info = {}
        
        try:
            # Get condition - UPDATED based on new HTML structure
            condition_value = "N/A"
            try:
                # Look for the new condition format using the provided HTML classes
                condition_xpaths = [
                    "//div[contains(@class, 'xu06os2')]//span[contains(@class, 'x193iq5w')]/span",
                    "//span[contains(@class, 'x6prxxf') and contains(@class, 'xvq8zen') and contains(@class, 'xo1l8bm')]",
                    "//span[contains(text(), 'Condition')]/../..//span[contains(@class, 'xzsf02u')]"
                ]
                
                for xpath in condition_xpaths:
                    elements = self.driver.find_elements(By.XPATH, xpath)
                    for element in elements:
                        text = element.text
                        if text and (text.lower() in ["new", "used", "good", "like new", "excellent", "fair", "poor"]):
                            condition_value = text
                            break
                    if condition_value != "N/A":
                        break
                
                # Fall back to the older methods if needed
                if condition_value == "N/A":
                    # Strategy 1: Look for condition label and its value
                    condition_labels = self.driver.find_elements(By.XPATH, 
                        "//span[contains(text(), 'Condition')]")
                    
                    if condition_labels:
                        # Try to find the value in the next element
                        condition_parent = condition_labels[0].find_element(By.XPATH, "./../../..")
                        condition_value_elements = condition_parent.find_elements(By.XPATH, 
                            ".//span[contains(@class, 'x193iq5w') and contains(@class, 'xo1l8bm')]")
                        
                        if condition_value_elements:
                            condition_value = condition_value_elements[0].text
                
                detailed_info["condition"] = condition_value
            except Exception as e:
                print(f"Error getting condition: {e}")
                detailed_info["condition"] = "N/A"
            
            # Get seller name - UPDATED based on new HTML structure
            try:
                seller_name = "N/A"
                # Try the exact class pattern from the example
                seller_xpaths = [
                    "//span[contains(@class, 'x193iq5w') and contains(@class, 'x6prxxf') and contains(@class, 'x1s688f') and contains(@class, 'xzsf02u')]",
                    "//span[contains(@class, 'x6prxxf') and contains(@class, 'xvq8zen') and contains(@class, 'x1s688f')]",
                    "//a[contains(@href, '/user/') or contains(@href, '/profile.php')]//span"
                ]
                
                for xpath in seller_xpaths:
                    elements = self.driver.find_elements(By.XPATH, xpath)
                    for element in elements:
                        text = element.text
                        if text and len(text) > 1 and " " in text:  # Likely a full name with space
                            seller_name = text
                            break
                    if seller_name != "N/A":
                        break
                        
                # Fallback to previous methods
                if seller_name == "N/A":
                    profile_elements = self.driver.find_elements(By.XPATH, 
                        "//a//image/../../..//span")
                    
                    for el in profile_elements:
                        text = el.text
                        if text and len(text) > 1:
                            seller_name = text
                            break
                
                detailed_info["seller_name"] = seller_name
            except Exception as e:
                print(f"Error getting seller name: {e}")
                detailed_info["seller_name"] = "N/A"
            
            # Get when seller joined Facebook
            try:
                joined_patterns = [
                    "//span[contains(text(), 'Joined Facebook')]",
                    "//span[contains(text(), 'on Facebook since')]",
                    "//span[contains(text(), 'Member since')]"
                ]
                
                joined_text = "N/A"
                for pattern in joined_patterns:
                    elements = self.driver.find_elements(By.XPATH, pattern)
                    if elements:
                        joined_text = elements[0].text
                        break
                
                detailed_info["seller_joined_date"] = joined_text
            except Exception as e:
                print(f"Error getting seller join date: {e}")
                detailed_info["seller_joined_date"] = "N/A"
            
            # Get description - UPDATED based on new HTML structure
            try:
                description = "N/A"
                # Try with the exact class pattern from the example
                desc_xpaths = [
                    "//span[@dir='auto' and contains(@class, 'x193iq5w') and contains(@class, 'x1lliihq') and contains(@class, 'xzsf02u')]",
                    "//span[contains(@class, 'x6prxxf') and contains(@class, 'xvq8zen') and contains(@class, 'xo1l8bm') and contains(@class, 'xzsf02u')]",
                    "//div[contains(@class, 'xu06os2')]//span[@dir='auto']"
                ]
                
                for xpath in desc_xpaths:
                    elements = self.driver.find_elements(By.XPATH, xpath)
                    for element in elements:
                        text = element.text
                        if text and len(text) > 20:  # Likely the description
                            description = text
                            break
                    if description != "N/A":
                        break
                
                # Fall back to previous methods if needed
                if description == "N/A":
                    # Strategy 1: Look for substantial text blocks
                    desc_patterns = [
                        "//span[contains(@class, 'x193iq5w') and contains(@class, 'x1n0sxbx')]",
                        "//div[contains(@class, 'xz9dl7a')]//span",  # Common description container
                        "//div[contains(@class, 'x1n2onr6')]//span[not(contains(@class, 'x1lliihq'))]"  # Another common pattern
                    ]
                    
                    for pattern in desc_patterns:
                        elements = self.driver.find_elements(By.XPATH, pattern)
                        for el in elements:
                            text = el.text
                            if text and len(text) > 20:  # Likely the main description
                                description = text
                                break
                        if description != "N/A":
                            break
                
                detailed_info["description"] = description
            except Exception as e:
                print(f"Error getting description: {e}")
                detailed_info["description"] = "N/A"
            
            # Get map location image
            try:
                map_url = "N/A"
                # Strategy 1: Background image
                map_elements = self.driver.find_elements(By.XPATH, 
                    "//div[contains(@style, 'background-image') and contains(@style, 'map')]")
                
                if map_elements:
                    bg_style = map_elements[0].get_attribute("style")
                    # Extract URL from background-image style
                    url_match = re.search(r'url\(["\']?(.*?)["\']?\)', bg_style)
                    if url_match:
                        map_url = url_match.group(1)
                
                # Strategy 2: Traditional map images
                if map_url == "N/A":
                    map_img_elements = self.driver.find_elements(By.XPATH, 
                        "//img[contains(@src, 'map') or contains(@src, 'static_map')]")
                    
                    if map_img_elements:
                        map_url = map_img_elements[0].get_attribute("src")
                
                detailed_info["map_location_image"] = map_url
            except Exception as e:
                print(f"Error getting map image: {e}")
                detailed_info["map_location_image"] = "N/A"
            
            # Get all listing images
            try:
                image_elements = self.driver.find_elements(By.XPATH, 
                    "//div[contains(@class, 'x6s0dn4')]//img")
                
                # If primary method fails, try alternative selectors
                if not image_elements:
                    image_elements = self.driver.find_elements(By.XPATH, "//img[contains(@class, 'x5yr21d')]")
                
                # If still no images, try any image with substantial size
                if not image_elements:
                    all_imgs = self.driver.find_elements(By.TAG_NAME, "img")
                    image_elements = [img for img in all_imgs if 
                                      img.get_attribute("width") and 
                                      int(img.get_attribute("width") or 0) > 100]
                
                listing_images = []
                for img in image_elements:
                    src = img.get_attribute("src")
                    if src and "http" in src:
                        listing_images.append(src)
                
                if listing_images:
                    detailed_info["all_images"] = listing_images
                else:
                    detailed_info["all_images"] = []
            except Exception as e:
                print(f"Error getting all images: {e}")
                detailed_info["all_images"] = []
                
        except Exception as e:
            print(f"Error getting listing details: {e}")
        
        # Close the tab and switch back to the main tab
        self.driver.close()
        self.driver.switch_to.window(self.driver.window_handles[0])
        
        return detailed_info
    
    def save_to_csv(self, df, filename="facebook_marketplace_listings.csv"):
        """Save the DataFrame to a CSV file"""
        try:
            df.to_csv(filename, index=False, encoding='utf-8')
            print(f"Data saved to {filename}")
        except Exception as e:
            print(f"Error saving to CSV: {e}")
    
    def close(self):
        """Close the browser"""
        self.driver.quit()