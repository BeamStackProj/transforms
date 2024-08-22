import apache_beam as beam
import logging
from urllib.parse import urlparse, urljoin
import time
from beamstack_transforms.utils import ImportParams, import_package, install_package

logger = logging.getLogger(__file__)
logging.basicConfig(level=logging.INFO)

REQUIRED_PACKAGES = [
    "beautifulsoup4==4.12.3", "requests"
]


class ScrapeWebPages(beam.PTransform):
    def __init__(self, max_depth: int = 1, min_char_size: int = 30):
        self.max_depth = max_depth
        self.min_char_size = min_char_size

    def expand(self, pcoll):
        return pcoll | beam.ParDo(self.ScrapeWebPage(self.max_depth, self.min_char_size))

    class ScrapeWebPage(beam.DoFn):
        def __init__(self, max_depth, min_char_size):
            self.max_depth = max_depth
            self.visited_urls = {}
            self.min_char_size = min_char_size

        def start_bundle(self):
            try:
                install_package(REQUIRED_PACKAGES)
                self.BeautifulSoupObj, self.requestsObj = import_package(
                    modules=[
                        ImportParams(
                            module="bs4",
                            objects=["BeautifulSoup"]
                        ),
                        ImportParams(
                            module="requests"
                        ),
                    ]
                )

            except Exception as e:
                logger.error("ERROR IMPORTING PACKAGE")
                logger.error(e)
                quit()

        def process(self, element):
            url = element.element
            base_domain = urlparse(url).netloc
            yield from self.scrape_page(url, base_domain, 0)

        def scrape_page(self, url, base_domain, depth):
            if depth > self.max_depth or url in self.visited_urls:
                return

            logger.info(f"Scraping URL: {url} at depth {depth}")
            self.visited_urls[url] = True

            try:
                response = self.requestsObj.get(url)
                response.raise_for_status()

                soup = self.BeautifulSoupObj(response.text, 'html.parser')

                for script_or_style in soup(['script', 'style']):
                    script_or_style.decompose()

                title = soup.title.string if soup.title else "No Title"

                for level in range(2, 7):
                    for heading in soup.find_all(f'h{level}'):
                        heading_text = heading.get_text()

                        segment = []
                        for sibling in heading.next_siblings:
                            if sibling.name and sibling.name.startswith('h'):
                                try:
                                    sibling_level = int(sibling.name[1])
                                    if sibling_level <= level:
                                        break
                                except ValueError:
                                    continue
                            if isinstance(sibling, str):
                                segment.append(sibling.strip())
                            else:
                                segment.append(sibling.get_text(strip=True))
                        segment_text = " ".join(segment).strip()

                        # Yield each heading and its corresponding text as a separate dictionary
                        if len(segment_text) > self.min_char_size:
                            yield {
                                "title": str(title),
                                "heading": str(heading_text),
                                "text": str(segment_text),
                                "url": str(url)
                            }

                for link in soup.find_all('a', href=True):
                    absolute_link = urljoin(url, link['href'])
                    link_domain = urlparse(absolute_link).netloc

                    if link_domain == base_domain:
                        yield from self.scrape_page(absolute_link, base_domain, depth + 1)

                time.sleep(.5)
            except self.requestsObj.exceptions.RequestException as e:
                logger.error(f"Error scraping {url}: {e}")
