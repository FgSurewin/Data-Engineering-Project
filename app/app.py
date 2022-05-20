from flask import Flask, render_template, request
from MySQKService import MySQLService
from dotenv import load_dotenv
from Tokenization import Get_freq
from SearchEngine import SearchEngine

load_dotenv()


app = Flask(__name__)


@app.route("/")
def home():
    return render_template("index.html")


@app.route("/show_result", methods=["POST"])
def show_result():
    ### Get keyword from post action
    keyword = request.form["keyword"]
    print(keyword)

    ### Init
    mysql_con = MySQLService()
    se = SearchEngine(mysql_con)

    ### Using search engine
    result = se.full_search(keyword, 10)

    sql_command = f"""
    select * from project_master.stage WHERE query_string='{keyword}' ORDER BY result_id DESC LIMIT 30
    """
    table_res = mysql_con.read_db(sql_command)
    print(table_res[:1])

    return_list = []
    for i in table_res:
        fun = Get_freq(keyword, i[2], i[1])
        if fun is not None and len(fun["keyword_freq"]) > 0:
            return_list.append(fun)

    return render_template("index.html", results=return_list)


if __name__ == "__main__":
    app.run()
