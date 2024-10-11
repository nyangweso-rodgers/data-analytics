reader = WikipediaReader(dir = "articles3")
reader.crawl("Artificial Intelligence", total_number = 200)
reader.crawl("Machine Learning")
reader.process()