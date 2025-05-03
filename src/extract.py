# fetch browser async client proxies
import json
import scrapy
from scrapy.crawler import CrawlerProcess
from collections import deque
from find_data import find_object
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

    # brought in values
    query = "shirts"
    city = "san diego"
    start_url = f'https://www.facebook.com/marketplace/{city}/search?query={query}&exact=false'
    seen = 0

    name = "marketplace_spider"
    custom_settings = {
        "USER_AGENT": None,
        "DOWNLOAD_HANDLERS": {
            "http": "scrapy_impersonate.handler.ImpersonateDownloadHandler",
            "https": "scrapy_impersonate.handler.ImpersonateDownloadHandler"
        },
        "TWISTED_REACTOR": "twisted.internet.asyncioreactor.AsyncioSelectorReactor",
    }

    # script content from page source
    script_content = ""
    payload_content = ""
 
    # values we need to extract
    document_id = ""
    city_id = ""

    fb_dtsg = ""
    lsd = ""
    jazoest = ""

    scale = 2
    count = 24
    savedSearchID = None
    savedSearchQuery = None
    contextual_data = None
    shouldIncludePopularSearches = False

    callsite = "COMMERCE_MKTPLACE_WWW"
    commerce_enable_local_pickup = True
    commerce_enable_shipping = True
    commerce_search_and_rp_available = True
    commerce_search_and_rp_category_id = []
    commerce_search_and_rp_condition = None
    commerce_search_and_rp_ctime_days = None
    filter_location_latitude = 0
    filter_location_longitude = 0
    filter_price_lower_bound = 0
    filter_price_upper_bound = 214748364700
    filter_radius_km = 0

    browse_context = None
    contexual_filters = []
    referral_code = None
    saved_search_strid = None
    search_vertical = "C2C"
    seo_url = None
    surface = "SEARCH"
    virtual_contextual_filters = []

    location_id = "category"
    url = None

    def start_requests(self):
        yield scrapy.Request(
            self.start_url,
            dont_filter=True,
            meta={"impersonate": "firefox135"}
        )

    def parse(self, response):
        print("THIS IS RESPONSE IN PARSE: ", response)
        # Find script tag with marketplace data
        raw_lsd = response.headers.get(b'x-fb-lsd')
        if raw_lsd:
            self.lsd = raw_lsd.decode()          # turn bytes → str
        else:
            self.lsd = None
        print("THIS IS LSD: ", self.lsd)

        self.fb_dtsg = response.xpath('//input[@name="fb_dtsg"]/@value').get()
        print("THIS IS FB_DTSG: ", self.fb_dtsg)
        self.jazoest = response.xpath('//input[@name="jazoest"]/@value').get()
        print("THIS IS JAZOEST: ", self.jazoest)
        script_tags = response.xpath('//script[@type="application/json" and @data-sjs]')
        self.logger.info(f"Found {len(script_tags)} script tags with type=application/json and data-sjs")
        
        for script in script_tags:
            content = script.get()
            if "marketplace_search" in content:
                self.script_content = script.xpath('./text()').get()
                
            if "filter_location_latitude" in content:
                self.payload_content = script.xpath('./text()').get()
                print("THIS IS PAYLOAD CONTENT: ", self.payload_content)

            if (self.script_content and self.payload_content):
                break
        
        if not (self.script_content and self.payload_content):
            self.logger.error("Could not find script tag with marketplace data")
            return
            
        self.setPayload()
        yield from self.finishParse()

    def setPayload(self):
        print("THIS IS SET PAYLOAD: ", self.payload_content)
        json_data = json.loads(self.payload_content)
        print("SCRIPT_CONTENT HERE", json_data)
        require = find_object(json_data, 'require')
        print("THIS IS REQUIRE for setPayload: ", require)
        __bbox = find_object(require, '__bbox')
        print("THIS IS __BBOX for setPayload: ")
        inner_require = find_object(__bbox, 'require')
        print("THIS IS INNER REQUIRE for setPayload: ", inner_require)
        expectedPreloaders = find_object(inner_require, 'expectedPreloaders')
        print("THIS IS EXPECTED PRELOADERS for setPayload: ", expectedPreloaders)
        variables = find_object(expectedPreloaders, 'variables', self.docId)
        print("THIS IS VARIABLES for setPayload: ", variables) 
        self.scale = variables.get('scale', 2)
        self.count = variables.get('count', 24)
        self.savedSearchID = variables.get('savedSearchID', None)
        self.savedSearchQuery = variables.get('savedSearchQuery', None)
        self.contextual_data = variables.get('contextual_data', None)
        self.shouldIncludePopularSearches = variables.get('shouldIncludePopularSearches', False)
        

        params = find_object(variables, 'params')
        print("THIS IS PARAMS for setPayload: ", params)
        bqf = find_object(params, 'bqf')
        print("THIS IS BQF for setPayload: ", bqf)
        self.callsite = bqf.get('callsite', "COMMERCE_MKTPLACE_WWW")
        print("THIS IS CALLSITE for setPayload: ", self.callsite)

        browse_request_params = find_object(params, 'browse_request_params')
        print("THIS IS BROWSER REQUEST PARAMS for setPayload: ", browse_request_params)
        self.commerce_enable_local_pickup = browse_request_params.get('commerce_enable_local_pickup', True)
        self.commerce_enable_shipping = browse_request_params.get('commerce_enable_shipping', True)
        self.commerce_search_and_rp_available = browse_request_params.get('commerce_search_and_rp_available', True)
        self.commerce_search_and_rp_category_id = browse_request_params.get('commerce_search_and_rp_category_id', [])
        self.commerce_search_and_rp_condition = browse_request_params.get('commerce_search_and_rp_condition', None)
        self.commerce_search_and_rp_ctime_days = browse_request_params.get('commerce_search_and_rp_ctime_days', None)
        self.filter_location_latitude = browse_request_params.get('filter_location_latitude', 0)
        self.filter_locatiolsn_longitude = browse_request_params.get('filter_location_longitude', 0)
        self.filter_price_lower_bound = browse_request_params.get('filter_price_lower_bound', 0)
        self.filter_price_upper_bound = browse_request_params.get('filter_price_upper_bound', 214748364700)
        self.filter_radius_km = browse_request_params.get('filter_radius_km', 0)
        
        custom_request_params = find_object(params, 'custom_request_params')
        print("THIS IS CUSTOM REQUEST PARAMS for setPayload: ", custom_request_params)
        self.browse_context = custom_request_params.get('browse_context', None)
        self.contexual_filters = custom_request_params.get('contexual_filters', [])
        self.referral_code = custom_request_params.get('referral_code', None)
        self.saved_search_strid = custom_request_params.get('saved_search_strid', None)
        self.search_vertical = custom_request_params.get('search_vertical', "C2C")
        self.seo_url = custom_request_params.get('seo_url', None)
        self.surface = custom_request_params.get('surface', "SEARCH")
        self.virtual_contextual_filters = custom_request_params.get('virtual_contextual_filters', [])

        topicPageParams = find_object(variables, 'topicPageParams')
        print("THIS IS TOPIC PAGE PARAMS for setPayload: ", topicPageParams)
        self.location_id = topicPageParams.get('location_id', "category")
        self.url = topicPageParams.get('url', None)



    def finishParse(self):
        print("scipt content outside of loop: ",self.script_content)
        
        try:
            # Parse JSON
            json_data = json.loads(self.script_content)
            print("json data IN TRY: ", json_data)
            
            # Direct navigation based on the structure you provided
            require = json_data.get('require', [])[0][3][0]
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
            print("THIS IS PAGE INFO and yeild IN PARSE: ")
            if self.should_build_next_request(page_info):
                print("BUILDING NEXT REQUEST")
                yield from self.build_next_request(end_cursor)
                
        except (json.JSONDecodeError, KeyError, IndexError) as e:
            self.logger.error(f"Error processing data: {e}")
            # If there's an error in navigation, yield the raw script for debugging
            yield {"raw_script": self.script_content[:1000] + "..." if self.script_content else None}

    def parse_graphql_response(self, response):
        print("THIS IS RESPONSE IN PARSE GRAPHQL RESPONSE: ", response)
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
            "buyLocation": {
                "latitude": self.filter_location_latitude,
                "longitude": self.filter_location_longitude,
            },
            "count": self.count,
            "cursor": cursor,
            "params": {
                "bqf": {
                    "callsite": self.callsite,
                    "query": self.query,
                },
                "browse_request_params": {
                    "commerce_enable_local_pickup": self.commerce_enable_local_pickup,
                    "commerce_enable_shipping": self.commerce_enable_shipping,
                    "commerce_search_and_rp_available": self.commerce_search_and_rp_available,
                    "commerce_search_and_rp_category_id": self.commerce_search_and_rp_category_id,
                    "commerce_search_and_rp_condition": self.commerce_search_and_rp_condition,
                    "commerce_search_and_rp_ctime_days": self.commerce_search_and_rp_ctime_days,
                    "filter_location_latitude": self.filter_location_latitude,
                    "filter_location_longitude": self.filter_location_longitude,
                    "filter_price_lower_bound": self.filter_price_lower_bound,
                    "filter_price_upper_bound": self.filter_price_upper_bound,
                    "filter_radius_km": self.filter_radius_km,
                },
                'custom_request_params': {
                    "browse_context": None,
                    "contextual_filters": [],
                    "referral_code": None,
                    "saved_search_strid": None,
                    "search_vertical": "C2C",
                    "seo_url": None,
                    "surface": "SEARCH",
                    "virtual_contextual_filters": []
                }
            },
            "topicPageParams": {
                "location_id": self.location_id,
                "url": self.url
            },
            "scale": self.scale,
            "savedSearchID": self.savedSearchID,
            "savedSearchQuery": self.savedSearchQuery,
            "contextual_data": self.contextual_data,
            "shouldIncludePopularSearches": self.shouldIncludePopularSearches
        }

        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Accept': '*/*',
            'Referer': self.start_url,
            'Origin': 'https://www.facebook.com',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36',
            'X-FB-Friendly-Name': 'CometMarketplaceSearchContentContainerQuery',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
        }

        if self.lsd:
            headers['X-FB-LSD'] = self.lsd

        form_data = {
        "av": "61569189908839",  # You can replace this with a default value
        "__user": "61569189908839",  # Same as above
        "__a": "1",
        "__req": "a",
        "dpr": "2",
        "__ccg": "EXCELLENT",
        "fb_api_caller_class": "RelayModern",
        "fb_api_req_friendly_name": "CometMarketplaceSearchContentContainerQuery",
        "variables": json.dumps(variables),
        "server_timestamps": "true",
        "doc_id": doc_id,
    }
        print("THIS IS FORM DATA: ", form_data)

        return [scrapy.FormRequest(
            url=graphql_url,
            formdata=form_data,
            headers=headers,
            callback=self.parse_graphql_response,
            dont_filter=True,
            meta={"impersonate": "firefox135"}
        )]
    
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
    
    def find_object(self, current, target):
        # Base case: if current is a dictionary, check if target is a key
        if isinstance(current, dict):
            if target in current:
                return current[target]
            # If not found directly, check each value
            for value in current.values():
                found = self.find_object(value, target)
                if found is not None:
                    return found
        
        # Handle array/list cases
        elif isinstance(current, list):
            for item in current:
                found = self.find_object(item, target)
                if found is not None:
                    return found
        
        # Target not found in this branch
        return None


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