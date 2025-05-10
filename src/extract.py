import json
import scrapy
from scrapy.crawler import CrawlerProcess


class MarketplaceSpider(scrapy.Spider):
    name = "marketplace_spider"
    custom_settings = {
        "USER_AGENT": None,
        "DOWNLOAD_HANDLERS": {
            "http": "scrapy_impersonate.handler.ImpersonateDownloadHandler",
            "https": "scrapy_impersonate.handler.ImpersonateDownloadHandler"
        },
        "TWISTED_REACTOR": "twisted.internet.asyncioreactor.AsyncioSelectorReactor",
    }

    def __init__(self, query="shirts", city="san diego", max_items=100, *args, **kwargs):
        super(MarketplaceSpider, self).__init__(*args, **kwargs)
        self.query = query
        self.city = city
        self.start_url = f'https://www.facebook.com/marketplace/{city}/search?query={query}&exact=false'
        self.max_items = max_items
        self.seen = 0

    def start_requests(self):
        yield scrapy.Request(
            self.start_url,
            dont_filter=True,
            meta={"impersonate": "firefox135"}
        )

    def parse(self, response):
        """Initial parse to extract GraphQL data from the page"""
        # Extract authentication tokens from response headers/body
        self.lsd = response.headers.get(b'x-fb-lsd', b'').decode()
        self.fb_dtsg = response.xpath('//input[@name="fb_dtsg"]/@value').get()
        self.jazoest = response.xpath('//input[@name="jazoest"]/@value').get()
        
        # Find script with GraphQL data - more targeted approach
        script_with_data = response.xpath('//script[@type="application/json" and @data-sjs and contains(., "marketplace_search")]/text()').get()
        
        if not script_with_data:
            self.logger.error("Could not find script tag with marketplace data")
            return
            
        # Parse the script content
        try:
            json_data = json.loads(script_with_data)
            
            # Extract the document ID (needed for GraphQL queries)
            self.document_id = self.extract_document_id(json_data)
            if not self.document_id:
                self.logger.error("Could not extract document ID")
                return
                
            # Extract GraphQL variables
            graphql_data = self.extract_graphql_data(json_data)
            if not graphql_data:
                self.logger.error("Could not extract GraphQL data")
                return
                
            # Process initial listings
            for edge in graphql_data.get("edges", []):
                yield self.extract_item(edge)
                
            # Check for pagination
            page_info = graphql_data.get("page_info", {})
            if self.should_fetch_more(page_info):
                yield from self.build_next_request(page_info.get("end_cursor"))
        
        except json.JSONDecodeError as e:
            self.logger.error(f"Error parsing JSON: {e}")
    
    def extract_document_id(self, json_data):
        """Extract the GraphQL document ID from the JSON data"""
        try:
            # Navigate through the JSON structure to find the document ID
            require = json_data.get('require', [])
            if require and len(require) > 0 and len(require[0]) > 3:
                bbox_item = require[0][3][0]
                if isinstance(bbox_item, dict) and '__bbox' in bbox_item:
                    inner_require = bbox_item.get('__bbox', {}).get('require', [])
                    if inner_require and len(inner_require) > 0 and len(inner_require[0]) > 3:
                        # The document ID is typically part of the preloader name
                        preloader_name = inner_require[0][3][0]
                        if isinstance(preloader_name, str) and "_" in preloader_name:
                            parts = preloader_name.split('_')
                            # Usually the document ID is the third part
                            if len(parts) >= 3:
                                return parts[2]
            
            # Alternative method - look for a direct reference
            doc_id_paths = [
                ["require", 0, 3, 0, "__bbox", "require", 0, 3, 0],
                ["require", 0, 3, 1, "__bbox", "define", 0, 0],
                ["__bbox", "require", 0, 3, 0]
            ]
            
            for path in doc_id_paths:
                try:
                    value = json_data
                    for key in path:
                        if isinstance(value, list) and isinstance(key, int) and len(value) > key:
                            value = value[key]
                        elif isinstance(value, dict) and key in value:
                            value = value[key]
                        else:
                            value = None
                            break
                    
                    if isinstance(value, str) and "_" in value:
                        parts = value.split("_")
                        if len(parts) >= 3 and parts[0] in ["MarketplaceSearch", "CometMarketplace"]:
                            return parts[2]
                except (IndexError, KeyError, TypeError):
                    continue
                    
            return None
        except Exception as e:
            self.logger.error(f"Error extracting document ID: {e}")
            return None
    
    def extract_graphql_data(self, json_data):
        """Extract marketplace search data from the JSON"""
        try:
            # Try to navigate to the marketplace_search data
            # This follows the structure you documented in your comments
            bbox_item = self.find_value_by_path(json_data, ["require", 0, 3, 0, "__bbox"])
            if not bbox_item:
                return None
                
            inner_require = self.find_value_by_path(bbox_item, ["require", 0, 3, 1, "__bbox"])    
            if not inner_require:
                return None
                
            result = inner_require.get("result", {})
            data = result.get("data", {})
            marketplace_search = data.get("marketplace_search", {})
            feed_units = marketplace_search.get("feed_units", {})
            
            # Extract location data for future requests
            viewer = feed_units.get("viewer", {})
            if viewer and "buy_location" in viewer:
                buy_location = viewer.get("buy_location", {}).get("buy_location", {})
                self.location_id = buy_location.get("id")
                location = buy_location.get("location", {})
                geocode = location.get("reverse_geocode", {})
                self.filter_location_latitude = location.get("latitude")
                self.filter_location_longitude = location.get("longitude")
            
            return feed_units
            
        except (KeyError, IndexError, AttributeError) as e:
            self.logger.error(f"Error extracting GraphQL data: {e}")
            return None
    
    def find_value_by_path(self, data, path):
        """Safely navigate a nested structure using a path"""
        current = data
        try:
            for part in path:
                if isinstance(current, dict) and part in current:
                    current = current[part]
                elif isinstance(current, list) and isinstance(part, int) and len(current) > part:
                    current = current[part]
                else:
                    return None
            return current
        except (KeyError, IndexError, TypeError):
            return None
    
    def build_next_request(self, cursor):
        """Build a GraphQL request for the next page of results"""
        graphql_url = "https://www.facebook.com/api/graphql/"
        
        # Build variables object for the GraphQL query
        variables = {
            "count": 24,  # Standard page size
            "cursor": cursor,
            "scale": 2,
            "params": {
                "bqf": {
                    "callsite": "COMMERCE_MKTPLACE_WWW",
                    "query": self.query,
                },
                "browse_request_params": {
                    "commerce_enable_local_pickup": True,
                    "commerce_enable_shipping": True,
                    "filter_location_latitude": self.filter_location_latitude,
                    "filter_location_longitude": self.filter_location_longitude,
                    "filter_price_lower_bound": 0,
                    "filter_price_upper_bound": 214748364700,
                    "filter_radius_km": 0
                },
                "custom_request_params": {
                    "search_vertical": "C2C",
                    "surface": "SEARCH"
                }
            },
            "topicPageParams": {
                "location_id": self.location_id if hasattr(self, 'location_id') else "category"
            }
        }

        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Accept': '*/*',
            'Referer': self.start_url,
            'Origin': 'https://www.facebook.com',
            'X-FB-Friendly-Name': 'CometMarketplaceSearchContentContainerQuery',
        }

        if hasattr(self, 'lsd') and self.lsd:
            headers['X-FB-LSD'] = self.lsd

        form_data = {
            "__a": "1",
            "__req": "a",
            "fb_api_caller_class": "RelayModern",
            "fb_api_req_friendly_name": "CometMarketplaceSearchContentContainerQuery",
            "variables": json.dumps(variables),
            "server_timestamps": "true",
            "doc_id": self.document_id,
        }
        
        if hasattr(self, 'fb_dtsg') and self.fb_dtsg:
            form_data["fb_dtsg"] = self.fb_dtsg
            
        if hasattr(self, 'jazoest') and self.jazoest:
            form_data["jazoest"] = self.jazoest

        self.logger.info(f"Requesting next page with cursor: {cursor[:20]}...")
        return [scrapy.FormRequest(
            url=graphql_url,
            formdata=form_data,
            headers=headers,
            callback=self.parse_graphql_response,
            dont_filter=True,
            meta={"impersonate": "firefox135"}
        )]
    
    def parse_graphql_response(self, response):
        """Parse subsequent GraphQL responses"""
        try:
            # GraphQL responses are usually JSON
            response_text = response.text
            
            # Some GraphQL responses have a prefix we need to remove
            if response_text.startswith("for (;;);"):
                response_text = response_text[9:]
                
            data = json.loads(response_text)
            
            # Navigate to the actual data
            marketplace_data = None
            if "data" in data:
                marketplace_data = data["data"].get("marketplace_search", {}).get("feed_units", {})
            else:
                # Try to find data in a different structure
                for key, value in data.items():
                    if isinstance(value, dict) and "data" in value:
                        if "marketplace_search" in value["data"]:
                            marketplace_data = value["data"]["marketplace_search"].get("feed_units", {})
                            break
            
            if not marketplace_data:
                self.logger.error("Could not find marketplace data in response")
                return
                
            # Process listings
            edges = marketplace_data.get("edges", [])
            for edge in edges:
                yield self.extract_item(edge)
                
            # Check for pagination
            page_info = marketplace_data.get("page_info", {})
            if self.should_fetch_more(page_info):
                yield from self.build_next_request(page_info.get("end_cursor"))
                
        except json.JSONDecodeError as e:
            self.logger.error(f"Error parsing GraphQL response: {e}")
            self.logger.debug(f"Response preview: {response.text[:200]}...")
    
    def should_fetch_more(self, page_info):
        """Determine if we should fetch more results"""
        return (
            self.seen < self.max_items and 
            page_info and 
            page_info.get("has_next_page", False) and 
            page_info.get("end_cursor")
        )
    
    def extract_item(self, edge):
        """Extract item details from an edge"""
        self.seen += 1
        
        node = edge.get("node", {})
        listing = node.get("listing", {})
        
        if not listing:
            return None
            
        # Extract all the fields we need
        return {
            'listing_id': listing.get('id'),
            'title': listing.get('marketplace_listing_title'),
            'price_amount': listing.get('listing_price', {}).get('amount'),
            'price_formatted': listing.get('listing_price', {}).get('formatted_amount'),
            'image_uri': listing.get('primary_listing_photo', {}).get('image', {}).get('uri'),
            'city': listing.get('location', {}).get('reverse_geocode', {}).get('city'),
            'state': listing.get('location', {}).get('reverse_geocode', {}).get('state'),
            'display_name': listing.get('location', {}).get('reverse_geocode', {}).get('city_page', {}).get('display_name'),
            'city_code': listing.get('location', {}).get('reverse_geocode', {}).get('city_page', {}).get('id'),
            'seller': listing.get('marketplace_listing_seller', {}).get('name'),
        }


if __name__ == "__main__":
    process = CrawlerProcess(
        settings={
            "FEEDS": {
                "marketplace_items.json": {
                    "format": "json",
                    "encoding": "utf8",
                    "indent": 4
                }
            },
            "LOG_LEVEL": "INFO"
        }
    )
    process.crawl(MarketplaceSpider, query="shirts", city="san diego", max_items=100)
    process.start()