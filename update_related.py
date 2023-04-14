import mysql.connector
import os
from dotenv import load_dotenv

TEST_MODE = False
# Allows us to access the .env file

if TEST_MODE:
    load_dotenv(".env.stage")
else:
    load_dotenv(".env.production")

DATABASE_DIRECTION = os.getenv("DATABASE_DIRECTION")
DATABASE_USER = os.getenv("DATABASE_USER")
DATABASE_PASSWORD = os.getenv("DATABSE_PASSWORD")
DATABASE_NAME = os.getenv("DATABASE_NAME")
DATABASE_TABLE = os.getenv("DATABASE_TABLE")


def updateRelatedQuestions():
    get_related_questions = "SELECT * FROM " + DATABASE_TABLE
    cnx = mysql.connector.connect(
        user=DATABASE_NAME,
        password=DATABASE_PASSWORD,
        host=DATABASE_DIRECTION,
        database=DATABASE_NAME,
    )
    cursor = cnx.cursor()
    cursor.execute(get_related_questions)
    myresult = cursor.fetchall()

    data = []
    for result in myresult:
        aux = list(result)
        aux[5] = aux[5].split(",")
        aux[6] = aux[6].split(",")
        data.append(aux)

    for each_message_i in data:
        scores = {}
        for each_message_j in data:
            if (
                each_message_i[0] != each_message_j[0]
                and len(set(each_message_i[6]).intersection(each_message_j[6])) > 0
            ):
                scores[each_message_j[0]] = len(
                    set(each_message_i[6]).intersection(each_message_j[6])
                )
        scores = sorted(scores.items(), key=lambda x: x[1], reverse=True)[:20]
        each_message_i[5] = []
        for score in scores:
            each_message_i[5].append(score[0])

    add_question = (
        "UPDATE " + DATABASE_TABLE + " SET related=%(related)s " "WHERE id=%(id)s"
    )

    for each_message in data:
        each_message[5] = str(",".join([item for item in each_message[5]][:10]))

        data_question = {"id": each_message[0], "related": each_message[5]}

        cursor.execute(add_question, data_question)

    cnx.commit()

    cursor.close()
    cnx.close()
