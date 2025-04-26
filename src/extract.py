# fetch browser async client proxies
import json
import scrapy
from scrapy.crawler import CrawlerProcess
# page source link: require":[["ScheduledServerJS","handle",null,[{"__bbox":{"require":[["RelayPrefetchedStreamCache","next",[],["adp_CometMarketplaceSearchContentContainerQueryRelayPreloade
# nested structure (title type): object {} -> require [] -> 0 [] -> 3 [] -> 0 {} -> __bbox {} -> require [] -> 0 [] -> 3 [] -> 1 {1} -> __bbox {3} -> result {2} -> data {3} -> marketplace_search {1} -> feed_units {4} -> edges [24] * data of postings -> 0 {3} -> node {6} -> listing {28}
# types inside of node 6 that I need below:
# listingId = id # note we must use https://www.facebook.com/marketplace/item/{listing_id}/ to get the full listing page
# primaryImage = primary_listing_photo {3} -> image {1} -> uri
# facebookPrice = listing_price {3} -> amount
# facebookPriceFormatted = listing_price {3} -> formatted_amount
# city = location {1} -> reverse_geocode {3} -> city
# state = location {1} -> reverse_geocode {3} -> state
# displayName = location {1} -> reverse_geocode {3} -> city_page {2} -> display_name
# cityLocationCode = location {1} -> reverse_geocode {3} -> city_page {2} -> id
# facebookTitle = marketplace_listing_title
# facebookSeller = marketplace_listing_seller {3} -> name


class Extract(scrapy.Spider):
    item = "shirts"
    city = "san diego"
    seen = 0
    document_id = ""
    city_id = ""

    name = "marketplace_spider"
    custom_settings = {
        "USER_AGENT": None,
        "DOWNLOAD_HANDLERS": {
            "http": "scrapy_impersonate.handler.ImpersonateDownloadHandler",
            "https": "scrapy_impersonate.handler.ImpersonateDownloadHandler"
        },
        "TWISTED_REACTOR": "twisted.internet.asyncioreactor.AsyncioSelectorReactor",
    }

    def start_requests(self):
        yield scrapy.Request(
            f'https://www.facebook.com/marketplace/{self.city}/search?query={self.item}&exact=false',
            dont_filter=True,
            meta={"impersonate": "firefox135"}
        )

    def parse(self, response):
    # Find script tag with marketplace data
        script_tags = response.xpath('//script[@type="application/json" and @data-sjs]')
        self.logger.info(f"Found {len(script_tags)} script tags with type=application/json and data-sjs")
        
        for script in script_tags:
            content = script.get()
            if "marketplace_search" in content:
                script_content = script.xpath('./text()').get()
                print("Found script content: ", script_content)
                break
        else:
            self.logger.error("Could not find script tag with marketplace data")
            return
        
        print("scipt content outside of loop: ",script_content)
        
        try:
            # Parse JSON
            json_data = json.loads(script_content)
            print("json data IN TRY: ", json_data)
            
            # Direct navigation based on the structure you provided
            require = json_data['require'][0][3][0]
            print("THIS IS REQUIRE: ", require)
            bbox_item = require.get('__bbox', {})
            print("THIS IS BBOX ITEM: ", bbox_item)
            parts = bbox_item.get('require', [])[0][3][0].split('_');
            self.document_id = parts[2];
            inner_require = bbox_item.get('require', [])[0][3][1]
            print("THIS IS INNER REQUIRE: ", inner_require)
            inner_bbox = inner_require.get('__bbox', {})
            print("THIS IS INNER BBOX: ", inner_bbox)
            result_item = inner_bbox.get('result', {})
            print("THIS IS RESULT ITEM: ", result_item)
            data_item = result_item.get('data', {})
            print("THIS IS DATA ITEM: ", data_item)
            marketplace_search = data_item['marketplace_search']
            print("THIS IS MARKETPLACE SEARCH: ", marketplace_search)
            feed_units = marketplace_search.get('feed_units', {})
            page_info = feed_units.get('page_info', {})
            print("THIS IS PAGE INFO: ", page_info)
            end_cursor = page_info.get('end_cursor', None)
            print("THIS IS END CURSOR: ", end_cursor)
            viewer = feed_units.get('viewer', {})
            buy_location = viewer.get('buy_location', {})
            buy_location_two = buy_location.get('buy_location', {})
            self.city_id = buy_location_two.get('id', None)
            print("THIS IS CITY ID: ", self.city_id)
            location = buy_location_two.get('location', {})
            print("THIS IS LOCATION: ", location)
            reverse_geocode = location.get('reverse_geocode', {})
            self.city = reverse_geocode.get('city', {})
            print("THIS IS CITY: ", self.city)
            has_next_page = page_info.get('has_next_page', False)
            print("THIS IS HAS NEXT PAGE: ", has_next_page)

            edges = feed_units.get('edges', [])
            print("THIS IS EDGES: ", edges)
            print ("LENGTH OF EDGES: ", len(edges))
            # Process each listing
            for edge in edges:
                yield self.extract_item(edge)

               # build the next request if there is a next page
            if self.should_build_next_request(page_info):
                yield from self.build_next_request(end_cursor)
                
        except (json.JSONDecodeError, KeyError, IndexError) as e:
            self.logger.error(f"Error processing data: {e}")
            # If there's an error in navigation, yield the raw script for debugging
            yield {"raw_script": script_content[:1000] + "..." if script_content else None}


    def parse_graphql_response(self, response):
        print("THIS IS HOW MANY SEEN: ", self.seen)
        try:
            data = json.loads(response.text).get('data', {})
            print("THIS IS DATA in parse_graphql_response: ", data)
            marketplace_search = data.get('marketplace_search', {})
            feed_units = marketplace_search.get('feed_units', {})
            edges = feed_units.get('edges', [])
            page_info = feed_units.get('page_info', {})
            for edge in edges:
                yield self.extract_item(edge)

            print("THIS IS PAGE INFO in parse_graphql_response: ", page_info)
            if self.should_build_next_request(page_info):
                cursor = page_info.get('end_cursor')
                yield from self.build_next_request(cursor)

        except json.JSONDecodeError as e:
            self.logger.error(f"Failed to parse GraphQL response: {e}")

    def build_next_request(self, cursor):
        graphql_url = "https://www.facebook.com/api/graphql/"
        doc_id = self.document_id

        variables = {
            "count": 24,
            "cursor": cursor,
            "params": {
                "bqf": {
                    "callsite": "COMMERCE_MKTPLACE_WWW",
                    "query_params": {
                        "city": self.city,
                        "query": self.item,
                        "exact": "false",
                        "city_id": self.city_id,
                    },
                    "browser_request_params": {
                        "city": self.city,
                        "query": self.item,
                        "exact": "false",
                        "city_id": self.city_id,
                    }
                }
            }
        }

        form_data = {
            "variables": json.dumps(variables),
            "doc_id": doc_id,
        }
        yield scrapy.FormRequest(
            url=graphql_url,
            formdata=form_data,
            callback=self.parse_graphql_response,
            meta={"impersonate": "firefox135"}
        )
    
    def should_build_next_request(self, page_info):
        return self.seen < 100 and page_info.get('has_next_page', False) and page_info.get('end_cursor', None)
    
    def extract_item(self, edge):
        node = edge.get('node', {})
        print("THIS IS NODE: ", node)
        listing = node.get('listing', {})
        print("THIS IS LISTING: ", listing)
        self.seen += 1;
                
                # Extract specific fields
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
        }
            


if __name__ == "__main__":
    process = CrawlerProcess(
        settings={
            "FEEDS": {
                "postings.json": {
                    "format": "json",
                    "append": True,
                }
            }
        }
    )
    process.crawl(Extract)
    process.start()

# 