from bs4 import BeautifulSoup
import requests, json
from datetime import datetime
import urllib.request
from bs4 import BeautifulSoup
from collections import Counter
import nltk
import uuid
from nltk.stem.snowball import SnowballStemmer


nltk.download("stopwords")


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
        try:
            self.google_search(keyword, num)
            self.bing_search(keyword, num)
            self.yahoo_search(keyword, num)
            results = {
                "GOOGLE": self._google_data,
                "BING": self._bing_data,
                "YAHOO": self._yahoo_data,
            }

            ## NUMBER OF LINKS WE SAVED ##
            link_counts = 0

            ## POPULATE STAGE TABLE ##
            sql_header = f"INSERT INTO {self.mysql_connector.db_name}.{self.mysql_connector.table_1_stage}(result_title,result_url,engine_id,is_ad,created_at,query_string) VALUES\n"
            sql_values = ""
            for key, value in results.items():

                for val in value:
                    link_counts += 1
                    sql_values += f"""('{val['title']}','{val['link']}',(SELECT engine_id FROM project_master.engine WHERE engine_name='{key}'),'0','{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}','{keyword}'),\n"""

            sql_values = sql_values[:-2] + ";"
            sql_statement = sql_header + sql_values
            # print(sql_statement)
            self.mysql_connector.update_db(sql_statement)

            ## TOKENIZATION ##
            # Query links from stage table
            first_sql_statement = f"""select * from project_master.stage WHERE query_string='{keyword}' ORDER BY result_id DESC LIMIT {link_counts};"""
            print(first_sql_statement)
            table_res = self.mysql_connector.read_db(first_sql_statement)
            print("------------------------------------")

            return_list = []

            for i in table_res:
                fun = self.Get_freq(keyword, i[2], i[1], i[3])
                # print("FUN -> ", fun)
                if fun is not None and len(fun["keyword_freq"]) > 0:
                    # Append return_list
                    fun["link_id"] = uuid.uuid4()
                    fun["keyword_id"] = uuid.uuid4()
                    fun["keyword"] = keyword
                    return_list.append(fun)

            ## POPULATE KEYWORD TABLE ##
            print("POPULATE KEYWORD TABLE ")
            sql_keyword_header = f"""INSERT INTO project_master.keyword VALUES\n"""
            sql_keyword_values = ""
            for result in return_list:
                sql_keyword_values += (
                    f"""('{result['keyword_id']}', '{result['keyword']}'),\n"""
                )

            sql_keyword_values = sql_keyword_values[:-2] + ";"
            sql_keyword_statement = sql_keyword_header + sql_keyword_values
            self.mysql_connector.update_db(sql_keyword_statement)
            # print("KEYWORD -> ", sql_keyword_statement)
            print("------------------------")

            ## POPULATE LINK TABLE ##
            print("POPULATE LINK TABLE ")
            sql_link_header = f"""INSERT INTO project_master.link (link_id, link_title, link_url, engine_id, click_time)  VALUES\n"""
            sql_link_values = ""
            for result in return_list:
                sql_link_values += f"""('{result['link_id']}', '{result['title']}', '{result['link']}', '{result['engine']}', {0}),\n"""

            sql_link_values = sql_link_values[:-2] + ";"
            sql_link_statement = sql_link_header + sql_link_values
            self.mysql_connector.update_db(sql_link_statement)
            print("------------------------")

            ## POPULATE FREQUENCY TABLE ##
            print("POPULATE FREQUENCY TABLE ")
            sql_freq_header = f"""INSERT INTO project_master.frequency (keyword_id, link_id, freq_value, word)  VALUES\n"""
            sql_freq_values = ""
            for result in return_list:
                for word, freq in result["keyword_freq"].items():
                    sql_freq_values += f"""('{result['keyword_id']}', '{result['link_id']}', '{freq}', '{word}'),\n"""

            sql_freq_values = sql_freq_values[:-2] + ";"
            sql_freq_statement = sql_freq_header + sql_freq_values
            self.mysql_connector.update_db(sql_freq_statement)
            print("------------------------")

            ## SORT RESULT BASED ON FREQUENCY ##
            return_list = sorted(
                return_list, key=lambda x: sum(x["keyword_freq"].values()), reverse=True
            )
        except:
            print("FOUND ERROR!")
            return None
        return return_list

    def print_google_search_results(self):
        print(len(self._google_data))
        print(json.dumps(self._google_data, indent=2, ensure_ascii=False))

    def print_yahoo_search_results(self):
        print(len(self._yahoo_data))
        print(json.dumps(self._yahoo_data, indent=2, ensure_ascii=False))

    def print_bing_search_results(self):
        print(len(self._bing_data))
        print(json.dumps(self._bing_data, indent=2, ensure_ascii=False))

    def Get_freq(self, keyword, link, title, engine):

        try:
            freq = {}

            stemmer = SnowballStemmer("english")

            # link = "https://www.ccny.cuny.edu/"
            raw_html = urllib.request.urlopen(link, timeout=5).read()
            soup = BeautifulSoup(raw_html, "lxml")

            for script in soup(["script", "style"]):
                script.decompose()

            text = soup.get_text()
            # print(text)

            lines = (line.strip() for line in text.splitlines())
            chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
            text = "\n".join(chunk for chunk in chunks if chunk)

            tokenizer = nltk.tokenize.RegexpTokenizer("\w+")
            tokens = tokenizer.tokenize(text)
            words = [word.lower() for word in tokens]
            sw = nltk.corpus.stopwords.words("english")
            words_ns = [word for word in words if word not in sw]

            # Old count
            # count = Counter(words_ns)

            count = Counter([stemmer.stem(stemmed) for stemmed in words_ns])
            all_freq = count.most_common()

            my_dict = {}
            # print("keyword -> ", keyword)
            split_keyword = keyword.lower().split(" ")
            # print("split_keyword -> ", split_keyword)
            for i in split_keyword:
                i = stemmer.stem(i)
                for j in all_freq:
                    if i == j[0]:
                        my_dict[i] = j[1]

        except:
            print("Link broken!")
            return None

        return {"keyword_freq": my_dict, "link": link, "title": title, "engine": engine}
