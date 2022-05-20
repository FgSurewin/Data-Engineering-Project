import urllib.request
from bs4 import BeautifulSoup
from collections import Counter
import nltk


nltk.download("stopwords")


def Get_freq(keyword, link, title):

    try:
        freq = {}

        # link = "https://www.ccny.cuny.edu/"
        raw_html = urllib.request.urlopen(link).read()
        soup = BeautifulSoup(raw_html)

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

        count = Counter(words_ns)

        all_freq = count.most_common()

        my_dict = {}

        split_keyword = keyword.lower().split(" ")

        for i in split_keyword:
            for j in all_freq:
                if i == j[0]:
                    my_dict[i] = j[1]

    except:
        print("Link broken!")
        return None

    return {"keyword_freq": my_dict, "link": link, "title": title}
