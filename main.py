import os
from slack_sdk import WebClient
from dotenv import load_dotenv
from fastapi.middleware.cors import CORSMiddleware
import datetime
import time
from pydantic import BaseModel
import mysql.connector
import logging
from fastapi import FastAPI, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
import importer
import update_related
from apscheduler.schedulers.background import BackgroundScheduler


headers = {
    "Content-type": "application/json",
}

# Additional configuration vars
USERS_BATCH_SIZE = 100
TEST_MODE = False
origins = ["*"]


def CheckLeap(year):
    # Checking if the given year is leap year
    if (year % 400 == 0) or (year % 100 != 0) and (year % 4 == 0):
        return 29
    else:
        return 28


# Allows us to access the .env file
if TEST_MODE:
    load_dotenv(".env.stage")
else:
    load_dotenv(".env.production")

# env variables
SLACK_APP_TOKEN = os.getenv("SLACK_APP_TOKEN")
TOKEN_FOR_SEARCH = os.getenv("TOKEN_FOR_SEARCH")
DATABASE_DIRECTION = os.getenv("DATABASE_DIRECTION")
DATABASE_USER = os.getenv("DATABASE_USER")
DATABASE_PASSWORD = os.getenv("DATABSE_PASSWORD")
DATABASE_NAME = os.getenv("DATABASE_NAME")
DATABASE_TABLE = os.getenv("DATABASE_TABLE")


client = WebClient(SLACK_APP_TOKEN)
app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    exc_str = f"{exc}".replace("\n", " ").replace("   ", " ")
    logging.error(f"{request}: {exc_str}")
    content = {"status_code": 10422, "message": exc_str, "data": None}
    return JSONResponse(
        content=content, status_code=status.HTTP_422_UNPROCESSABLE_ENTITY
    )


def formatWorkspaceUsers(users):
    cleanedUsers = {}
    for user in users:
        cleanedUsers[user["id"]] = user
    return cleanedUsers


def getUsersInChannelFromSlack(client, channelID):
    users_in_channel = []
    all_users_fetched = False

    # Cursor needed to iter over the requests (pagination)
    users_cursor = ""

    # Get all users id that pertain to the channel
    while all_users_fetched is False:
        users_retrieved = client.conversations_members(
            channel=channelID, limit=USERS_BATCH_SIZE, cursor=users_cursor
        ).data

        users_in_channel += users_retrieved["members"]

        all_users_fetched = not (
            users_retrieved["response_metadata"]
            and users_retrieved["response_metadata"]["next_cursor"]
        )
        users_cursor = users_retrieved["response_metadata"]["next_cursor"]

    # [id_user_1, id_user_2, ... ]
    return users_in_channel


@app.get("/")
def testMessage():
    return True


@app.get("/v1/getQuestions/{query}")
def getMessages(query):
    cnx = mysql.connector.connect(
        user=DATABASE_NAME,
        password=DATABASE_PASSWORD,
        host=DATABASE_DIRECTION,
        database=DATABASE_NAME,
    )
    cursor = cnx.cursor()

    substring_query = (
        "SELECT * FROM " + DATABASE_TABLE + " WHERE "
        "LOWER(question) LIKE %(search_for)s OR "
        "LOWER(answer) LIKE %(search_for)s OR "
        "LOWER(keywords) LIKE %(search_for)s OR "
        "LOWER(aux_keywords) LIKE %(search_for)s"
    )
    cursor.execute(substring_query, {"search_for": "%" + query + "%"})
    myresult = cursor.fetchall()
    cursor.close()
    cnx.close()
    return myresult


@app.get("/v1/users/{channelID}")
def getUsersInChannel(channelID):
    temp_workspace_users = getUsersInWorkspace(client)
    workspace_users = formatWorkspaceUsers(temp_workspace_users)
    temp_channel_users = getUsersInChannelFromSlack(client, channelID)
    channel_users = [
        workspace_users[channel_user] for channel_user in temp_channel_users
    ]
    data = {"channel": channel_users}
    return data


class Item(BaseModel):
    question: str
    answer: str
    user: str


class ItemRelated(BaseModel):
    ids: str


@app.get("/v1/totalQuestionsAvailable")
def getTotalQuestions():
    get_query = "SELECT COUNT(*) FROM " + DATABASE_TABLE
    cnx = mysql.connector.connect(
        user=DATABASE_NAME,
        password=DATABASE_PASSWORD,
        host=DATABASE_DIRECTION,
        database=DATABASE_NAME,
    )

    cursor = cnx.cursor()
    cursor.execute(get_query)
    myresult = cursor.fetchall()
    cursor.close()
    cnx.close()

    return myresult[0][0]


@app.get("/v1/updateDatabase")
def updateDatabase():
    importer.importToDB()
    return 200


@app.get("/v1/updateRelatedDatabase")
def updateRelatedDatabase():
    # Prevent race condition
    time.sleep(300)
    update_related.updateRelatedQuestions()
    return 200


@app.post("/v1/getRelatedQuestions")
def getRelatedQuestions(item: ItemRelated):
    ids = item.ids.split(",")
    for index, id in enumerate(ids):
        ids[index] = str(id)
    ids = tuple(ids)
    cnx = mysql.connector.connect(
        user=DATABASE_NAME,
        password=DATABASE_PASSWORD,
        host=DATABASE_DIRECTION,
        database=DATABASE_NAME,
    )
    cursor = cnx.cursor()
    statement = "SELECT * FROM {0} WHERE id IN ({1})".format(
        DATABASE_TABLE, ", ".join(["%s"] * len(ids))
    )

    cursor.execute(statement, tuple(ids))
    myresult = cursor.fetchall()
    cursor.close()
    cnx.close()
    return {"status": 200, "related": myresult}


@app.post("/v1/createQuestion")
def sendMessageToChannel(item: Item):
    try:
        add_question = (
            "INSERT INTO " + DATABASE_TABLE + " "
            "(id, question, answer, user_question,user_answer,related,keywords,aux_keywords,score) "
            "VALUES (%(id)s, %(question)s, %(answer)s, %(user_question)s,%(user_answer)s,"
            "%(related)s,%(keywords)s,%(aux_keywords)s,%(score)s)"
        )

        cnx = mysql.connector.connect(
            user=DATABASE_NAME,
            password=DATABASE_PASSWORD,
            host=DATABASE_DIRECTION,
            database=DATABASE_NAME,
        )

        cursor = cnx.cursor()

        data_question = {
            "id": time.time(),
            "question": item.question,
            "answer": item.answer,
            "user_question": item.user,
            "user_answer": item.user,
            "related": "",
            "keywords": "",
            "aux_keywords": "",
            "score": 0,
        }

        cursor.execute(add_question, data_question)
        cnx.commit()
        cursor.close()
        cnx.close()
        return 200
    except mysql.connector.Error as err:
        print(err)
        return 500


@app.get("/v1/messages/{channelID}/{month}/{year}")
def getMessagesInChannel(channelID, month, year, reactions=""):
    allMessages = getAllMessagesFromTheChannel(channelID, month, year)
    # messages_array,messages_totals=getMessagesWithSpecificReactions(allMessages)
    messages_array = {}
    messages_array["all_messages"] = allMessages
    # messages_array.update(messages_totals)

    return messages_array


def getMessagesWithSpecificReactions(allMessages):
    messages_in_channel = {}
    messages_in_channel_totals = {}
    for message in allMessages:
        if "reactions" in message:
            for reaction in message["reactions"]:
                if ("all_messages_:" + reaction["name"] + ":") in messages_in_channel:
                    messages_in_channel[
                        "all_messages_:" + reaction["name"] + ":"
                    ].append(message)
                    messages_in_channel_totals[
                        "all_messages_:" + reaction["name"] + ":" + "_total"
                    ] += reaction["count"]
                else:
                    messages_in_channel["all_messages_:" + reaction["name"] + ":"] = [
                        message
                    ]
                    messages_in_channel_totals[
                        "all_messages_:" + reaction["name"] + ":" + "_total"
                    ] = reaction["count"]
    return messages_in_channel, messages_in_channel_totals


def getUsersInWorkspace(client):
    users_in_workspace = []
    users_batch = 200
    all_users_fetched = False

    # Cursor needed to iter over the requests (pagination)
    users_cursor = ""

    # Get all users id that pertain to the channel
    while all_users_fetched is False:
        users_retrieved = client.users_list(
            limit=users_batch, cursor=users_cursor, include_locals=True
        ).data
        users_in_workspace += users_retrieved["members"]
        all_users_fetched = not (
            users_retrieved["response_metadata"]
            and users_retrieved["response_metadata"]["next_cursor"]
        )
        users_cursor = users_retrieved["response_metadata"]["next_cursor"]

    # [{user_1}, {user_2}, ... ]
    return users_in_workspace


def getAllMessagesFromTheChannel(channel_id, month, year):
    months = [
        "",
        "january",
        "february",
        "march",
        "april",
        "may",
        "june",
        "july",
        "august",
        "september",
        "october",
        "november",
        "december",
    ]
    days_months = {
        "january": 31,
        "february": CheckLeap(int(year)),
        "march": 31,
        "april": 30,
        "may": 31,
        "june": 30,
        "july": 31,
        "august": 31,
        "september": 30,
        "october": 31,
        "november": 30,
        "december": 31,
    }

    datetime.time()
    latest = datetime.datetime(
        int(year), months.index(month), days_months[month], 23, 59, 59, 999999
    )
    oldest = datetime.datetime(int(year), months.index(month), 1)

    # Generate the timestamps that are required to make the request in the Slack API
    oldest = time.mktime(oldest.timetuple())
    latest = time.mktime(latest.timetuple())
    # Prints to debug (We can use logger or others later).
    # print("Tomorrow timestamp: "+str(tomorrow))
    # print("Month ago  timestamp: "+str(month_ago))

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
    return all_messages


sched = BackgroundScheduler(daemon=True)
sched.add_job(updateDatabase,'cron',week='*')
sched.add_job(updateRelatedDatabase,'cron',week='*')
sched.start()

if __name__ == "__main__":
    # uvicorn.run("server.api:app", host="0.0.0.0", port=8000, reload=True)
    app.run(debug=True, port=8000)
