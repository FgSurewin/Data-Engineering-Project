from flask import Flask, render_template, request, jsonify
from MySQLService import MySQLService
from dotenv import load_dotenv
from Tokenization import Get_freq
from SearchEngine import SearchEngine
from flask_cors import CORS
import time

load_dotenv()


app = Flask(__name__)
CORS(app)


@app.route("/")
def home():
    return render_template("react.html")


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

    return render_template("index.html", results=result)


@app.route("/api/history_links", methods=["POST"])
def history_links():
    curret_keyword = request.form.get("keyword", None)
    mysql_con = MySQLService()
    sql_satement = f"""
            SELECT DISTINCT l.link_id, l.link_title, l.link_url, l.engine_id, l.click_time
            FROM project_master.link l
            INNER JOIN project_master.frequency f
            ON l.link_id = f.link_id
            INNER JOIN project_master.keyword k
            ON k.keyword_id = f.keyword_id
            WHERE k.text = '{curret_keyword}' AND l.click_time > 0
            ORDER BY l.click_time DESC;

    """
    select_result = mysql_con.read_db(sql_satement)
    if len(select_result) > 0:
        select_result = [
            dict(
                link_id=link_id,
                title=link_title,
                link=link_url,
                engine=engine_id,
                click_time=click_time,
            )
            for link_id, link_title, link_url, engine_id, click_time in select_result
        ]
    return jsonify(select_result)


@app.route("/api/search_links", methods=["POST"])
def search_links():
    ### Get keyword from post action
    keyword = request.form["keyword"]

    ### Init
    mysql_con = MySQLService()
    se = SearchEngine(mysql_con)

    ### Using search engine
    result = se.full_search(keyword, 10)
    return jsonify(result)


@app.route("/api/update_link", methods=["POST"])
def update_link():
    ### Get keyword from post action
    link_id = request.form["link_id"]

    ### Init
    mysql_con = MySQLService()
    update_result = mysql_con.update_db(
        f"UPDATE project_master.link SET click_time = click_time + 1 WHERE link_id = '{link_id}'"
    )
    return jsonify("Update successfully")


if __name__ == "__main__":
    app.run(debug=True)
