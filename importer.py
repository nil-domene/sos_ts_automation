import mysql.connector
import nltk
from rake_nltk import Rake
from itertools import chain
from nltk.corpus import wordnet
import re
import os
import datetime
import time
from slack_sdk import WebClient
from dotenv import load_dotenv

nltk.download("stopwords")
nltk.download("punkt")
nltk.download("wordnet")


rake_class = Rake()

MESSAGE_BATCH_SIZE = 100
TEST_MODE = False

# Allows us to access the .env file
if TEST_MODE:
    load_dotenv(".env.stage")
else:
    load_dotenv(".env.production")

# env variables
SLACK_APP_TOKEN = os.getenv("SLACK_APP_TOKEN")
SLACK_CHANNEL_ID = os.getenv("SLACK_CHANNEL_ID")
CHANNEL_NAME = os.getenv("CHANNEL_NAME")
TOKEN_FOR_SEARCH = os.getenv("TOKEN_FOR_SEARCH")
DATABASE_DIRECTION=os.getenv("DATABASE_DIRECTION")
DATABASE_USER=os.getenv("DATABASE_USER")
DATABASE_PASSWORD=os.getenv("DATABSE_PASSWORD")
DATABASE_NAME=os.getenv("DATABASE_NAME")
DATABASE_TABLE=os.getenv("DATABASE_TABLE")

client = WebClient(SLACK_APP_TOKEN)
channel_id = SLACK_CHANNEL_ID


def CheckLeap(year):
    # Checking if the given year is leap year
    if (year % 400 == 0) or (year % 100 != 0) and (year % 4 == 0):
        return 29
    else:
        return 28


# both in timestamps
def getQuestions(init_date, final_date):
    # Generate the timestamps that are required to make the request in the Slack API
    oldest = time.mktime(init_date.timetuple())
    latest = time.mktime(final_date.timetuple())

    # Request to the Slack API
    all_messages = []
    cursor_id = None
    intitial_loop = True
    while cursor_id is not None or intitial_loop:
        intitial_loop = False
        result = client.conversations_history(
            channel=channel_id,
            latest=latest,
            oldest=oldest,
            inclusive=True,
            include_all_metadata=True,
            limit=500,
            cursor=cursor_id,
        )
        all_messages += result.data["messages"]
        if result.data["has_more"] is True:
            cursor_id = result.data["response_metadata"]["next_cursor"]
        else:
            cursor_id = None
    print(all_messages)

    # Check for done and remove if true
    cleaned_questions = []
    for message in all_messages:
        addToQuestions = True
        if "reactions" in message:
            for reaction in message["reactions"]:
                if reaction["name"] == "done1":
                    addToQuestions = False
            if addToQuestions:
                cleaned_questions.append(message)

    return cleaned_questions


def getResponses(init_date, final_date):
    responses = []
    actual_page = 0
    final_page = 0
    init_date_formatted = init_date.strftime("%Y-%m-%d")
    final_date_formatted = final_date.strftime("%Y-%m-%d")

    # Get all users id that pertain to the channel
    while actual_page <= final_page:
        data_retrieved = client.search_messages(
            page=actual_page,
            count=MESSAGE_BATCH_SIZE,
            token=TOKEN_FOR_SEARCH,
            query="in:sos_ts is:thread has::green_check_mark: before:{} after:{} ".format(
                final_date_formatted, init_date_formatted
            ),
        )

        responses += data_retrieved.data["messages"]["matches"]

        if final_page == -1:
            final_page = data_retrieved.data["messages"]["paging"]["pages"]

        actual_page += 1

    return responses


def importData(init_date, final_date):
    questions = getQuestions(init_date, final_date)
    responses = getResponses(init_date, final_date)
    return questions, responses


def extractKeywords(questions):
    cleanedData = []
    for each_message in questions:
        if "subtype" not in each_message:
            rake_class.extract_keywords_from_text(each_message["text"])
        extracted_keyword = rake_class.get_ranked_phrases()
        aux = []
        for each_extracted_keyword in extracted_keyword:
            synonyms = wordnet.synsets(each_extracted_keyword)
            aux = list(
                set(chain.from_iterable([word.lemma_names() for word in synonyms]))
            )
        each_message["keywords"] = extracted_keyword
        upper_case_words = re.split(r"\s+[a-z][a-z\s]*", each_message["text"])
        each_message["keywords"].extend([x.lower() for x in upper_case_words])
        each_message["aux_keywords"] = aux
        each_message["related"] = []
        cleanedData.append(each_message)
    return cleanedData


def enrichData(questions, responses):
    cleanedData = extractKeywords(questions)
    for each_response in responses:
        for each_enriched_question in cleanedData:
            if (
                "permalink" in each_enriched_question
                and "permalink" in each_response
                and each_enriched_question["permalink"] == each_response["permalink"]
            ):
                each_enriched_question["response"] = each_response
                break
            if (
                "thread_ts" in each_enriched_question
                and "permalink" in each_response
                and each_enriched_question["thread_ts"]
                == each_response["permalink"].split("thread_ts=")[1]
            ):
                each_enriched_question["response"] = each_response
                break

    finalData = []
    for question in cleanedData:
        if "response" in question:
            question["keywords"] = [str(i) for i in question["keywords"]]
            for index, keyword in enumerate(question["keywords"]):
                if len(keyword) > 0 and keyword[0] == ":" and keyword[-1] == ":":
                    question["keywords"].remove(keyword)
                elif len(keyword) == 0:
                    del question["keywords"][index]
            finalData.append(question)

    return finalData


def insertData(data):
    cnx = mysql.connector.connect(
        user=DATABASE_NAME, password=DATABASE_PASSWORD, host=DATABASE_DIRECTION, database=DATABASE_NAME
    )

    cursor = cnx.cursor()

    add_question = (
        "INSERT INTO " + DATABASE_TABLE + " "
        "(id, question, answer, user_question,user_answer,related,keywords,aux_keywords,score) "
        "VALUES (%(id)s, %(question)s, %(answer)s, %(user_question)s,%(user_answer)s,"
        "%(related)s,%(keywords)s,%(aux_keywords)s,%(score)s)"
    )

    for element in data:
        data_question = {
            "id": element["ts"],
            "question": element["text"][:10000],
            "answer": element["response"]["text"][:10000],
            "user_question": element["user"],
            "user_answer": element["response"]["user"],
            "related": str(",".join([item[0] for item in element["related"]][:10])),
            "keywords": str(",".join(element["keywords"])),
            "aux_keywords": str(",".join(element["aux_keywords"])),
            "score": 0,
        }
        cursor.execute(add_question, data_question)

    cnx.commit()
    cursor.close()
    cnx.close()


# MAIN
def importToDB():
    day = datetime.datetime.now().day
    month = datetime.datetime.now().month
    year = datetime.datetime.now().year

    final_date = datetime.datetime(int(year), month, day, 23, 59, 59, 999999)
    init_date = datetime.datetime(int(year), month, day, 0, 0, 0) - datetime.timedelta(
        days=7
    )

    questions, responses = importData(init_date, final_date)
    enrichedData = enrichData(questions, responses)
    insertData(enrichedData)
