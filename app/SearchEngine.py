from bs4 import BeautifulSoup
import requests, json
from datetime import datetime


class SearchEngine:
    def __init__(self, mysql_connector) -> None:
        # self._keywords = "+".join(keywords.split(" ")) # sample: Climate+Change
        # self._num = num
        self.mysql_connector = mysql_connector
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.54 Safari/537.36",
        }
        self._google_data = []
        self._yahoo_data = []
        self._bing_data = []

    def keyword_parser(self, keyword):
        return "+".join(keyword.split(" "))

    def replace_quote(self, item):

        return str(item).replace("'", "")

    def google_search(self, keyword, num=10):
        # Clean cached data
        self._google_data = []

        html = requests.get(
            "https://www.google.com/search?q="
            + self.keyword_parser(keyword)
            + "&num="
            + str(num),
            headers=self.headers,
        )
        soup = BeautifulSoup(html.text, "lxml")
        linkElements = soup.select_one("#search").select_one("div").find("div")

        # Check if there are complex results
        real_linkElements = (
            linkElements.select(".TzHB6b") if len(linkElements) <= 1 else linkElements
        )

        for link in real_linkElements:
            try:
                title_tags = link.select("h3")
                for title_tag in title_tags:
                    a_tag = title_tag.find_parent("a") if title_tag else None
                    if title_tag != None and a_tag != None:
                        self._google_data.append(
                            {
                                "title": self.replace_quote(title_tag.text),
                                "link": a_tag["href"],
                            }
                        )
            except:
                continue

        print(f"Result from Goolge search: {len(self._google_data)}")

    # Huihuang
    def yahoo_search(self, keyword, num=10):
        # Clean cached data
        self._yahoo_data = []

        for i in range(int(num / 10)):

            html = requests.get(
                "https://search.yahoo.com/search?p="
                + self.keyword_parser(keyword)
                + "&pz="
                + str(10)
                + "&b="
                + str(i * 10),
                headers=self.headers,
            )
            soup = BeautifulSoup(html.text, "lxml")
            h3s = soup.findAll("h3")

            for item in h3s:

                try:
                    sub_soup = BeautifulSoup(
                        str(item.a).replace(str(item.a.span), ""), "lxml"
                    )

                    if sub_soup.a.text is None:
                        continue
                    self._yahoo_data.append(
                        {
                            "title": self.replace_quote(sub_soup.a.text),
                            "link": sub_soup.a["href"],
                        }
                    )

                except:
                    continue
        print(f"Result from Yahoo search: {len(self._yahoo_data)}")

    # Juan
    def bing_search(self, keyword, num=10):
        # Clean cached data
        self._bing_data = []

        html = requests.get(
            "https://www.bing.com/search?q="
            + self.keyword_parser(keyword)
            + "&count="
            + str(num),
            headers=self.headers,
        )
        soup = BeautifulSoup(html.text, "lxml")
        h2s = soup.findAll("h2")

        for item in h2s:
            title = ""
            link = ""

            try:
                if (
                    "videos/search" in item.a["href"]
                    or "news/search" in item.a["href"]
                    or "images/search" in item.a["href"]
                ):
                    link = "https://bing.com" + item.a["href"]
                else:
                    link = item.a["href"]

                title = item.text

                self._bing_data.append(
                    {"title": self.replace_quote(title), "link": link}
                )

            except:
                continue
        print(f"Result from Bing search: {len(self._bing_data)}")

    def full_search(self, keyword, num=10):
        self.google_search(keyword, num)
        self.bing_search(keyword, num)
        self.yahoo_search(keyword, num)
        results = {
            "GOOGLE": self._google_data,
            "BING": self._bing_data,
            "YAHOO": self._yahoo_data,
        }

        ## POPULATE STAGE TABLE ##

        sql_header = f"INSERT INTO {self.mysql_connector.db_name}.{self.mysql_connector.table_1_stage}(result_title,result_url,engine_id,is_ad,created_at,query_string) VALUES\n"
        sql_values = ""
        for key, value in results.items():

            for val in value:
                sql_values += f"""('{val['title']}','{val['link']}',(SELECT engine_id FROM project_master.engine WHERE engine_name='{key}'),'0','{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}','{keyword}'),\n"""

        sql_values = sql_values[:-2] + ";"
        sql_statement = sql_header + sql_values
        # print(sql_statement)

        self.mysql_connector.update_db(sql_statement)

    def print_google_search_results(self):
        print(len(self._google_data))
        print(json.dumps(self._google_data, indent=2, ensure_ascii=False))

    def print_yahoo_search_results(self):
        print(len(self._yahoo_data))
        print(json.dumps(self._yahoo_data, indent=2, ensure_ascii=False))

    def print_bing_search_results(self):
        print(len(self._bing_data))
        print(json.dumps(self._bing_data, indent=2, ensure_ascii=False))
