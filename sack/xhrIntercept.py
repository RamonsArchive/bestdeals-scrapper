# fb_marketplace_scraper.py
import json
import requests
import pandas as pd
import time

def build_headers(c_user, xs, fr, datr):
    return {
        "accept": "*/*",
        "accept-encoding": "gzip, deflate, br, zstd",
        "accept-language": "en-US,en;q=0.9",
        "content-type": "application/x-www-form-urlencoded",
        "origin": "https://www.facebook.com",
        "referer": "https://www.facebook.com/marketplace/",
        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
        "x-fb-friendly-name": "CometMarketplaceSearchContentContainerQuery",
        "x-fb-lsd": "zlOx-8WfLgSgmDMsP9-rML",
        "cookie": f"c_user={c_user}; xs={xs}; fr={fr}; datr={datr};"
    }

def build_payload(search_term, location_id=None):
    variables = {
        "count": 24,
        "cursor": None,
        "entityID": "marketplace",
        "scale": 1,
        "searchQuery": search_term,
        "surface": "MARKETPLACE",  # Changed from COMPOSER to MARKETPLACE
        "timezone": -420
    }
    
    # Add location ID if provided
    if location_id:
        variables["buyLocation"] = {
            "latitude": 0,
            "longitude": 0,
            "locationID": location_id
        }
    
    return {
        "fb_api_caller_class": "RelayModern",
        "fb_api_req_friendly_name": "CometMarketplaceSearchContentContainerQuery",
        "doc_id": "7135233673201201",  # Subject to change by Facebook
        "variables": json.dumps(variables),
        # Add server_timestamps parameter
        "server_timestamps": "true"
    }

def fetch_marketplace_data(headers, payload, url="https://www.facebook.com/api/graphql/"):
    print("Sending request with headers:")
    print(json.dumps(headers, indent=2))
    print("\nPayload:")
    print(json.dumps(payload, indent=2))
    
    try:
        # Add a small delay before making the request
        time.sleep(1)
        
        response = requests.post(url, headers=headers, data=payload)
        status_code = response.status_code
        print(f"Response status code: {status_code}")
        
        # Save the raw response to a file for debugging
        with open("raw_response.txt", "wb") as f:
            f.write(response.content)
        
        # Print the first 500 characters of the response for debugging
        print("Response preview:")
        preview = response.text[:500] + "..." if len(response.text) > 500 else response.text
        print(preview)
        
        response.raise_for_status()
        
        # Check if response is valid JSON
        try:
            data = response.json()
            return data
        except json.JSONDecodeError as e:
            print(f"Invalid JSON response: {e}")
            print("Response might be HTML instead of JSON. Check for CAPTCHA or login redirects.")
            return None
            
    except requests.RequestException as e:
        print(f"Request failed: {e}")
        return None

def parse_listings(response):
    if not response:
        return pd.DataFrame()
    
    try:
        response_text = response.text
        
        # Try to parse the JSON response
        try:
            data = json.loads(response_text)
        except json.JSONDecodeError:
            print("Failed to parse JSON, response might be HTML or error page")
            # Save response to file for examination
            with open("failed_response.html", "w", encoding="utf-8") as f:
                f.write(response_text)
            return pd.DataFrame()
        
        # Check if we have the expected data structure
        if "data" not in data or "marketplace_search" not in data.get("data", {}):
            print("Unexpected response structure. API format may have changed.")
            print("Response keys:", data.keys())
            return pd.DataFrame()
        
        # Get the listings
        try:
            listings = data["data"]["marketplace_search"]["feed_units"]["edges"]
        except KeyError:
            print("Could not find listings in the response. Structure:")
            print(json.dumps(data["data"], indent=2)[:500] + "...")
            return pd.DataFrame()
        
        extracted = []
        for edge in listings:
            try:
                node = edge.get("node", {})
                listing = node.get("listing", {})
                
                if not listing:
                    continue
                    
                # Extract price information
                price_amount = listing.get("price_amount", {})
                price = f"{price_amount.get('currency', 'USD')} {price_amount.get('amount', 0)}"
                
                # Extract location info
                location = listing.get("location", {})
                reverse_geocode = location.get("reverse_geocode", {})
                city = reverse_geocode.get("city", "")
                state = reverse_geocode.get("state", "")
                
                # Extract image URL
                primary_photo = listing.get("primary_listing_photo", {})
                image = primary_photo.get("image", {})
                image_url = None
                
                # Try to get the highest resolution image
                if "uri_images" in image:
                    uri_images = image.get("uri_images", {})
                    for size in ["original", "480", "360", "240"]:
                        if size in uri_images:
                            image_url = uri_images[size]
                            break
                # Fallback to URI if uri_images is not available
                if not image_url:
                    image_url = image.get("uri", "")
                
                # Extract seller info
                seller = listing.get("marketplace_listing_seller", {})
                seller_name = seller.get("name", "")
                
                extracted.append({
                    "Title": listing.get("marketplace_listing_title", ""),
                    "Price": price,
                    "City": city,
                    "State": state,
                    "Seller": seller_name,
                    "Image URL": image_url,
                    "Listing ID": listing.get("id", ""),
                    "URL": f"https://www.facebook.com/marketplace/item/{listing.get('id', '')}/",
                    "Description": listing.get("description", "")
                })
            except (KeyError, TypeError) as e:
                print(f"Error processing a listing: {e}")
                continue
        
        if not extracted:
            print("No listings found in the response")
            return pd.DataFrame()
            
        return pd.DataFrame(extracted)
        
    except Exception as e:
        print(f"Error parsing listings: {e}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame()

def main():
    search_term = input("What are you looking for on Marketplace? ")
    
    # TODO: Load these securely or from environment variables
    c_user = "61569189908839"
    xs = "11%3ArSmA7KP8QAWzoQ%3A2%3A1744960514%3A-1%3A3932%3A%3AAcV9tj5gi06xfB3dDxADukOnaL8U4wJ3Z61qUEjMkX8"
    fr = "1IFvDUgVgyCan1iIH.AWcob5FSQkoXyUOYVxiywc86PWdVU341pye1dW34SZ-N0v2MdRY.BoBKUm..AAA.0.0.BoBKUm.AWcIeHI0hb6gTnqSwbE4JA1kdhg"
    datr = "R1LiZ_ZKc9Ym86019x4m-fJN"
    
    # San Diego location ID
    location_id = "108142219218141"
    
    headers = build_headers(c_user, xs, fr, datr)
    payload = build_payload(search_term, location_id)
    
    response = fetch_marketplace_data(headers, payload)
    
    if response:
        df = parse_listings(response)
        
        if not df.empty:
            print("\nFound listings:")
            print(df)
            
            # Save to CSV
            output_file = f"marketplace_{search_term.replace(' ', '_')}.csv"
            df.to_csv(output_file, index=False)
            print(f"\nResults saved to {output_file}")
        else:
            print("No listings were found or could be parsed")
    else:
        print("Failed to get a valid response from Facebook")

if __name__ == "__main__":
    main()