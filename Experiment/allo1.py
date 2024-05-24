import pandas as pd
import numpy as np
import mysql.connector

def allocation1():
    connection = mysql.connector.connect(
                host = "localhost",
                port = 3306,
                user = 'root',
                database = "expandinghorizons",
                password = "dB79@dG2024!"
    )

    cursor = connection.cursor()

    query = ("UPDATE child "
             "SET assigned = 0")
    cursor.execute(query)
    connection.commit()

    query = ("UPDATE ei "
             "SET Allo_Count = 0")
    cursor.execute(query)
    connection.commit()

    query = ("UPDATE locationdataalgo1 "
             "SET assigned = 0")
    cursor.execute(query)
    connection.commit()

    query = ("SELECT child_id from child")
    cursor.execute(query)

    child_ids = cursor.fetchall()

    for child in child_ids:
        query = ("Select ld.child_id_1, ld.ei_id_1, ld.assigned, ld.driving_distance, ld.driving_time, child.Longitude, child.Latitude, child.assigned, ei.Allo_Count from locationdataalgo1 ld "
                 "join child on child.child_id=ld.child_id_1 "
                 "join ei on ei.ei_id = ld.ei_id_1 "
                 f"where ld.child_id_2 is null and ld.ei_id_2 is null and child.assigned = 0 and ld.child_id_1 = '{child[0]}' and ld.ei_id_1 != '65d75e28e09e67e025e8cd3c'"
                 "order by driving_distance asc")
        cursor.execute(query)
        child_data = cursor.fetchall()

        if child_data:
            for child in child_data:
                ei = child[1]
                longitude = child[5]
                latitude = child[6]
                alloCount = child[8]
                break
            alloCount += 1

            updateQuery = ("UPDATE locationdataalgo1 ld "
                           "JOIN child on ld.child_id_1 = child.child_id "
                           "JOIN ei on ei.ei_id = ld.ei_id_1 "
                           f"SET ld.assigned = 1, child.assigned = 1, ei.Allo_Count = {alloCount} " 
                           f"WHERE child.longitude = {longitude} and child.latitude = {latitude} and ld.ei_id_1 = '{ei}'")
            cursor.execute(updateQuery)
            connection.commit()
            #print("Update query pushed")
        else:
            query = ("Select ld.child_id_1, ld.ei_id_1, ld.assigned, ld.driving_distance, ld.driving_time, child.Longitude, child.Latitude, child.assigned from locationdataalgo1 ld "
                     "join child on child.child_id=ld.child_id_1 "
                     f"where ld.child_id_2 is null and ld.ei_id_2 is null and child.assigned = 1 and ld.child_id_1 = '{child[0]}' "
                     "order by driving_distance asc")
            cursor.execute(query)
            data = cursor.fetchall()
            
            '''
            if data:
                print(f"{child[0]} already assigned")
            else:
                print("Error")
            '''

    cursor.close()
    connection.close()