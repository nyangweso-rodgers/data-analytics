# Scrapping Data From Google Search

## Table Of Contents
- [Further Reading]()
  - [Data Science Central - Scraping Data from Google Search Using Python and Scrapy](https://www.datasciencecentral.com/scrape-data-from-google-search-using-python-and-scrapy-step-by/)

# Description
* Scrapping Google SERPs (Search Engine Result Pages)

# Applications for Google Scrapper
* We can scrap Google SERPs to:
    1. Collect Customer Feedback Data to Inform Your Marketing
    2. Inform Your SEO and PPC Strategy
    3. Generate Content Ideas

# Project Goal(s)
1. Build a Google Web Scrapper to collect competitor's reviews.

# Steps
1. Choose target keywords to support the main goal
    * To pick your target keywords, think of the terms consumers could be searching to find your offering, and identify your competitors. In this example, we can use the following keywords
      * "best project management software"
2. Setup Development Environment
3. Get API Key from ScraperAPI. To get it, just [create a free ScraperAPI account](https://dashboard.scraperapi.com/signup) to redeem 5000 free API requests.
4. Create Project Folder
   * After installing __Scrapy__ in the __Python's Virtual Environment__, enter the below snippet in the terminal to create the necessary folders:
        ```sh
            scrapy startproject google_scraper
            cd google_scraper
            scrapy genspider google api.scraperapi.com
        ```
    * __Scrapy__ will first create a new project folder called “__google-scraper__,” which also happens to be the project’s name. Next, go into this folder and run the “genspider” command to create a web scraper named “google”.
    * We now have many configuration files, a “spiders” folder containing our scraper, and a Python modules folder containing package files.

5. Import All Necessary Dependencies to the __google.py__ File.
   * The next step is to build a few components that will make our script as efficient as possible. To do so, we’ll need to make our dependencies available to our scraper by adding them at the top of our file:
        ```py
            import scrapy
            from urllib.parse import urlencode
            from urllib.parse import urlparse
            import json
            from datetime import datetime

            API_KEY = 'YOUR_API_KEY'
        ```
    * With these dependencies in place, we can use them to build requests and handle JSON files. This last detail is important because we’ll be using ScraperAPI’s autoparse functionality.
    * After sending the HTTP request, it will return the data in JSON format, simplifying the process and making it so that we don’t have to write and maintain our own parser.

6. __Step 6: Construct the Google Search Query__: 
   * Google employs a standard and query-able URL structure. You just need to know the URL parameters for the data you need and you can generate a URL to query Google with.
  
   * That said, the following makes up the URL structure for all Google search queries: `http://www.google.com/search`
  
   * There are several standard parameters that make up Google search queries:
     * __q__ represents the __search keyword__ parameter. `http://www.google.com/search?q=tshirt`, for example, will look for results containing the keyword “__tshirt__.”
     * The offset point is specified by the __start__ parameter. `http://www.google.com/search?q=tshirt&start=100` is an example.
     * __hl__ is the language parameter. `http://www.google.com/search?q=tshirt&hl=en` is a good example.
     * The __as_sitesearch__ argument allows you to search for a domain (or website). `http://www.google.com/search?q=tshirt&as` sitesearch=amazon.com is one example.
     * The number of results per page (maximum is 100) is specified by the num parameter. `http://www.google.com/search?q=tshirt&num=50` is an example.
     * The __safe__ parameter generates only “safe” results. `http://www.google.com/search?q=tshirt&safe=active` is a good example.

    * Alright, let’s define a method to construct our Google URL using this information:
      ```py
            def create_google_url(query, site=''):
                google_dict = {'q': query, 'num': 100, }

                if site:

                    web = urlparse(site).netloc

                    google_dict['as_sitesearch'] = web

                    return 'http://www.google.com/search?' + urlencode(google_dict)

                return 'http://www.google.com/search?' + urlencode(google_dict)
      ````
    * In our method we’re setting ‘q’ as query because we’ll specify our actual keywords later in the script to make it easier to make changes to our scraper.

7. __STEP 7: Define the Scraper API Method__:
    * To use ScraperAPI, all we need to do is to send our request through ScraperAPI’s server by appending our query URL to the proxy URL provided by ScraperAPI using payload and urlencode. The code looks like this:

        ```py
            def get_url(url):
                payload = {'api_key': API_KEY, 'url': url, 'autoparse': 'true', 'country_code': 'us'}

                proxy_url = 'http://api.scraperapi.com/?' + urlencode(payload)

                return proxy_url
        ```
    * Now that we have defined the logic our scraper will use to construct our target URLs, it’s time to build the main spider.

8. __STEP 8: Write the Spider Class__:
   * In Scrapy we can create different classes, called spiders, to scrape specific pages or groups of sites. Thanks to this function, we can build different spiders inside the same project, making it much easier to scale and maintain.

        ```py
            class GoogleSpider(scrapy.Spider):
                name = 'google'

                allowed_domains = ['api.scraperapi.com']

                custom_settings = {'ROBOTSTXT_OBEY': False, 'LOG_LEVEL': 'INFO',

                                'CONCURRENT_REQUESTS_PER_DOMAIN': 10, 

                                'RETRY_TIMES': 5}
        ```
    * We need to give our spider a name, as this is how Scrapy will determine which script you want to run. The name you choose should be specific to what you’re trying to scrape, as projects with multiple spiders can get confusing if they aren’t clearly named. 
    * Because our URLs will start with ScraperAPI’s domain, we’ll also need to add “api.scraper.com” to allowed_domains. ScraperAPI will change the IP address and headers between every retry before returning a failed message (which doesn’t count against our total available API calls).
    * We also want to tell our scraper to ignore the directive in the robots.txt file. This is because by default Scrapy won’t scrape any site which has a contradictory directive inside said file.
    * Finally, we’ve set a few constraints so that we don’t exceed the limits of our free ScraperAPI account. As you can see in the custom_settings code above, we’re telling ScraperAPI to send 10 concurrent requests and to retry 5 times after any failed response.

9. __STEP 9: Send the Initial Requests__:
    * It’s finally time to send our HTTP request. It is very simple to do this with the start_requests(self) method:

        ```py
            def start_requests(self):
                queries = ['asana+reviews', 'clickup+reviews', 'best+project+management+software', 'best+project+management+software+for+small+teams']

                    url = create_google_url(query)

                    yield scrapy.Request(get_url(url), callback=self.parse, meta={'pos': 0})
        ```
    * It will loop through a list of queries that will be passed to the create_google_url function as query URL keywords.
    * The query URL we created will then be sent to Google Search via the proxy connection we set up in the get_url function, utilizing Scrapy’s yield. The result will then be given to the parse function to be processed (it should be in JSON format). The {‘pos’: 0} key-value pair is also added to the meta parameter, which is used to count the number of pages scraped.
    * __Note__: when typing keywords, remember that every word in a keyword is separated by a + sign, rather than a space.

10. __STEP 10:  Write the Parse Function__:
    * Thanks to ScraperAPI’s auto parsing functionality, our scraper should be returning a JSON file as a response to our request. Make sure it is by enabling the parameter ‘autoparse’: ‘true’ in the get_url function.
    * Next, we’ll load the complete JSON response and cycle through each result, taking the data and combining it into a new item that we can utilize later.

        ```py
            def parse(self, response):
                di = json.loads(response.text)

                pos = response.meta['pos']

                dt = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

                for result in di['organic_results']:

                    title = result['title']

                    snippet = result['snippet']

                    link = result['link']

                    item = {'title': title, 'snippet': snippet, 'link': link, 'position': pos, 'date': dt}

                    pos += 1

                    yield item

                next_page = di['pagination']['nextPageUrl']

                if next_page:

                    yield scrapy.Request(get_url(next_page), callback=self.parse, meta={'pos': pos})
        ```

        * This procedure checks to see whether another page of results is available. The request is invoked again if an additional page is present, repeating until there are no additional pages.

11. __STEP 11: Run the Spider__: 
    * To run our scraper, navigate to the project’s folder inside the terminal and use the following command:
      ```sh
        scrapy crawl google -o serps.csv
      ```

      * Now our spider will run and store all scraped data in a new CSV file named “serps.” This feature is  a big time saver and one more reason to use Scrapy for web scraping Google.
      * The stored data can then be analyzed and used to provide insight for tools, marketing and more.

# Remarks
* _[Moz’s comprehensive list of google search parameters](https://moz.com/blog/the-ultimate-guide-to-the-google-search-parameters) is incredibly useful in building a query-able URL. Bookmark it for more complex scraping projects in the future._