import pandas as pd
import numpy as np
import mysql.connector
from sklearn.preprocessing import MinMaxScaler
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import silhouette_score, davies_bouldin_score, calinski_harabasz_score
from sklearn.metrics import mutual_info_score, adjusted_rand_score

def buildDataFrame(table):
    connection = mysql.connector.connect(
                host = "localhost",
                port = 3306,
                user = 'root',
                database = "expandinghorizons",
                password = "dB79@dG2024!"
    )

    cursor = connection.cursor()

    query = ("SELECT distinct child_id FROM child ")
    cursor.execute(query)

    child_ids = cursor.fetchall()

    data_list = []
    for row in child_ids:
        data = {}
        data["id"] = row[0]
        child1_child_query = ("SELECT child_id_1, child_id_2, driving_distance, driving_time "
                              f"FROM {table} "
                              f"WHERE child_id_1 = '{row[0]}' AND ei_id_1 IS NULL AND ei_id_2 IS NULL")
        cursor.execute(child1_child_query)
        child1_child = cursor.fetchall()

        for row2 in child1_child:
            # Add columns with prefixes
            data[f'{row2[1]}_distance'] = row2[2]
            data[f'{row2[1]}_time'] = row2[3]

        child_child2_query = ("SELECT child_id_1, child_id_2, driving_distance, driving_time "
                              f"FROM {table} "
                              f"WHERE child_id_2 = '{row[0]}' AND ei_id_1 IS NULL AND ei_id_2 IS NULL")
        cursor.execute(child_child2_query)
        child_child2 = cursor.fetchall()

        for row3 in child_child2:
            data[f'{row3[0]}_distance'] = row3[2]
            data[f'{row3[0]}_time'] = row3[3]

        child_ei_query = ("SELECT child_id_1, ei_id_1, driving_distance, driving_time "
                          f"FROM {table} "
                          f"WHERE child_id_1 = '{row[0]}' AND child_id_2 IS NULL AND ei_id_2 IS NULL")
        cursor.execute(child_ei_query)
        child_ei = cursor.fetchall()

        for row4 in child_ei:
            data[f'{row4[1]}_distance'] = row4[2]
            data[f'{row4[1]}_time'] = row4[3]

        assigned_query = ("SELECT ei_id_1 "
                          f"FROM {table} "
                          f"WHERE child_id_1 = '{row[0]}' and assigned = 1")
        cursor.execute(assigned_query)
        assigned = cursor.fetchall()

        data["assigned_ei"] = assigned[0][0]

        data_list.append(data)

    query = ("SELECT ei_id FROM ei")
    cursor.execute(query)

    ei_ids = cursor.fetchall()
    for row5 in ei_ids:
        data["id"] = row5[0]
        ei_child_query = ("SELECT ei_id_1, child_id_1, driving_distance, driving_time "
                          f"from {table} "
                          f"WHERE ei_id_1 = '{row5[0]}' and child_id_2 is NULL and ei_id_2 is NULL")
        cursor.execute(ei_child_query)
        ei_child = cursor.fetchall()
        for row6 in ei_child:
            data[f'{row6[1]}_distance'] = row6[2]
            data[f'{row6[1]}_time'] = row6[3]

        ei1_ei_query = ("SELECT ei_id_1, ei_id_2, driving_distance, driving_time "
                        f"from {table} "
                        f"WHERE ei_id_1 = '{row5[0]}' and child_id_1 is NULL and child_id_2 is NULL")
        cursor.execute(ei1_ei_query)
        ei1_ei = cursor.fetchall()
        for row7 in ei1_ei:
            data[f'{row7[1]}_distance'] = row7[2]
            data[f'{row7[1]}_time'] = row7[3]

        ei_ei2_query = ("SELECT ei_id_1, ei_id_2, driving_distance, driving_time "
                        f"from {table} "
                        f"WHERE ei_id_2 = '{row5[0]}' and child_id_1 is NULL and child_id_2 is NULL")
        cursor.execute(ei_ei2_query)
        ei_ei2 = cursor.fetchall()
        for row8 in ei1_ei:
            data[f'{row8[0]}_distance'] = row8[2]
            data[f'{row8[0]}_time'] = row8[3]

        data["assigned_ei"] = row5[0]

        data_list.append(data)

    cursor.close()
    connection.close()

    df = pd.DataFrame(data_list)

    return df

def processData(df):
    df = df.fillna(0)
    popped_column = df.pop("assigned_ei")
    df["assigned_ei"] = popped_column
    df = df.drop(columns=["id"])
    
    Y = df["assigned_ei"].values
    X = df.drop(["assigned_ei"], axis=1)
    
    scaler = MinMaxScaler()
    X_scaled = scaler.fit_transform(X)
    
    return X_scaled, Y

def relationMetrics(table):
    connection = mysql.connector.connect(
                host = "localhost",
                port = 3306,
                user = 'root',
                database = "expandinghorizons",
                password = "dB79@dG2024!"
    )

    cursor = connection.cursor()
    
    query = ("Select ldr.child_id_1, ldr.ei_id_1, lda.ei_id_1 "
             "from locationdatareal ldr "
             f"join {table} lda on ldr.child_id_1 = lda.child_id_1 "
             "where ldr.assigned = 1 and lda.assigned = 1")
    
    cursor.execute(query)
    
    allocation = cursor.fetchall()
    
    df = pd.DataFrame(allocation, columns=["child_id", "real_ei", "allocation_ei"])
    
    label_encoder = LabelEncoder()
    
    encoded_categories = label_encoder.fit_transform(df["real_ei"])
    df["real_ei_encoded"] = encoded_categories
    df["allocation_ei_encoded"] = label_encoder.transform(df["allocation_ei"])
    
    real = df["real_ei_encoded"]
    allocation = df["allocation_ei_encoded"]
    
    ari = adjusted_rand_score(real, allocation)
    mi = mutual_info_score(real, allocation)
    
    confusion_matrix = pd.crosstab(real, allocation, rownames=["Real"], colnames=["Allocated"])
    correct_predictions = confusion_matrix.values.diagonal().sum()
    total_predictions = confusion_matrix.values.sum()
    accuracy = correct_predictions / total_predictions
    
    print(confusion_matrix)
    cursor.close()
    connection.close()
    
    return ari, mi, accuracy

def calculate_wcss_euclidean(X_scaled, Y):
    wcss = 0
    clusters = np.unique(Y)
    
    for cluster in clusters:
        cluster_points = X_scaled[Y == cluster]
        centroid = cluster_points.mean(axis=0)
        wcss += np.sum((cluster_points - centroid) ** 2)
    
    return wcss

def calculate_wcss_manhattan(X_scaled, Y):
    wcss = 0
    clusters = np.unique(Y)
    
    for cluster in clusters:
        cluster_points = X_scaled[Y == cluster]
        centroid = cluster_points.mean(axis=0)
        # Calculate Manhattan distance for each point to the centroid
        distances = np.sum(np.abs(cluster_points - centroid), axis=1)
        wcss += np.sum(distances)
    
    return wcss

def runClusterMetrics(table):
    data = buildDataFrame(table)
    X, Y = processData(data)
    ari, mi, accuracy = relationMetrics(table)
    
    silhouetteE = silhouette_score(X, Y)
    silhouetteM = silhouette_score(X, Y, metric="manhattan")
    db_index = davies_bouldin_score(X, Y)
    ch_index = calinski_harabasz_score(X, Y)
    wcssE = calculate_wcss_euclidean(X, Y)
    wcssM = calculate_wcss_manhattan(X, Y)

    print(f'Silhouette Score Euclidean: {silhouetteE}')
    print(f"Silhouette Score Manhattan: {silhouetteM}")
    print(f"Davies-Bouldin Index: {db_index}")
    print(f"Calinski-Harabasz Index: {ch_index}")
    print(f"Within-Cluster Sum of Squares Euclidean: {wcssE}")
    print(f"Within-Cluster Sum of Squares Manhattan: {wcssM}")
    print(f"Adjusted Rand Index: {ari}")
    print(f"Mutual Information (MI): {mi}")
    print(f"Accuracy: {accuracy}")
    
    return silhouetteE, silhouetteM, db_index, ch_index, wcssE, wcssM, ari, mi, accuracy