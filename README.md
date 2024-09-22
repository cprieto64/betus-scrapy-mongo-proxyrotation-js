## BetUs Races Scraper

This scraper extracts race information from the BetUs website and stores it in a MongoDB database. It utilizes Selenium for handling dynamic content and Scrapy for efficient web crawling.

### Inputs

* **URL:** The starting URL is the BetUs racebook page. It utilizes Selenium to extract links to individual races.
* **MongoDB Credentials:** The scraper requires MongoDB connection details for storing the scraped data. You need to configure the MongoDB connection string in the code. 

### Outputs

The scraper stores the following race data in a MongoDB database collection called 'horse_races':

* **race_id:** A unique identifier for each race generated using a hash of the race name, number, and date.
* **race_name:** The name of the race.
* **race_number:** The number of the race.
* **race_date_source:** The original date and time extracted from the BetUs website.
* **race_date:** The race date converted to GMT timezone.
* **horse_number:** The horse number in the race.
* **runner_name:** The name of the horse runner.
* **jockey:** The name of the jockey.
* **ml:** The morning line odds of the horse.
* **win:** The win odds for the horse.
* **place:** The place odds for the horse.
* **show:** The show odds for the horse.
* **wager_type:** The wager type associated with the race.
* **horses_results:** The results of the horses in the race.
* **updated_at:** The timestamp when the data was last updated.
* **source:** Identifies the source of the data, in this case, 'betus'.

**Additionally, the scraper also saves results of races into 'horse_races_results' collection only if results are available.** 

### Usage

1. Configure your MongoDB connection string in the code.
2. Ensure you have Scrapy, Selenium, and other necessary packages installed.
3. Run the scraper using the command `scrapy crawl betusraces`.
4. The scraper will automatically crawl the BetUs racebook page, extract race information, and store it in the MongoDB database.

**Note:** This scraper is designed to be scheduled to run regularly to keep the MongoDB database updated with the latest race information.
