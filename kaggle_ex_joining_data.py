from google.cloud import bigquery

# Create a "Client" object
client = bigquery.Client()

# Construct a reference to the "stackoverflow" dataset
dataset_ref = client.dataset("stackoverflow", project="bigquery-public-data")

# API request - fetch the dataset
dataset = client.get_dataset(dataset_ref)

# Get a list of available tables 
tables = list(client.list_tables(dataset))
list_of_tables = [table.table_id for table in tables] 

# Construct a reference to the "posts_answers" table
answers_table_ref = dataset_ref.table("posts_answers")

# API request - fetch the table
answers_table = client.get_table(answers_table_ref)

# Preview the first five lines of the "posts_answers" table
client.list_rows(answers_table, max_results=5).to_dataframe()

# Construct a reference to the "posts_questions" table
questions_table_ref = dataset_ref.table("posts_questions")

# API request - fetch the table
questions_table = client.get_table(questions_table_ref)

# Preview the first five lines of the "posts_questions" table
client.list_rows(questions_table, max_results=5).to_dataframe()

# compile dataset based on tags
questions_query = """
                  SELECT id, title, owner_user_id
                  FROM `bigquery-public-data.stackoverflow.posts_questions`
                  WHERE tags LIKE '%bigquery%'
                  """
# configure query
safe_config = bigquery.QueryJobConfig(maximum_bytes_billed=10**10)
questions_query_job = client.query(
    questions_query, job_config=safe_config)

# API request - run the query, and return a pandas DataFrame
questions_results = questions_query_job.to_dataframe()

# QA
print(questions_results.head())

answers_query = """
                SELECT a.id, a.body, a.owner_user_id
                FROM `bigquery-public-data.stackoverflow.posts_questions` as q
                INNER JOIN `bigquery-public-data.stackoverflow.posts_answers` as a
                    ON q.id = a.parent_id 
                WHERE q.tags like '%bigquery%'
                """

# query config
safe_config = bigquery.QueryJobConfig(maximum_bytes_billed=27*10**10)
answers_query_job = client.query(
    answers_query, job_config=safe_config)

# API request - run the query, and return a pandas DataFrame
answers_results = answers_query_job.to_dataframe()

# Preview results
print(answers_results.head())

# query for accounts with more than one answer
bigquery_experts_query = """
                        SELECT a.owner_user_id AS user_id, COUNT(1) AS number_of_answers
                        FROM `bigquery-public-data.stackoverflow.posts_answers` AS a
                        INNER JOIN `bigquery-public-data.stackoverflow.posts_questions` AS q
                            ON a.parent_id = q.id
                        WHERE q.tags LIKE '%bigquery%'
                        GROUP BY user_id
                        """

# Set up the query
safe_config = bigquery.QueryJobConfig(maximum_bytes_billed=10**10)
bigquery_experts_query_job = client.query(
    bigquery_experts_query, job_config=safe_config)

# API request - run the query, and return a pandas DataFrame
bigquery_experts_results = bigquery_experts_query_job.to_dataframe()

# Preview results
print(bigquery_experts_results.head())



def expert_finder(topic, client):
    """
    Returns a DataFrame with the user IDs who have written Stack Overflow answers on a topic.

    Inputs:
        topic: A string with the topic of interest
        client: A Client object that specifies the connection to the Stack Overflow dataset

    Outputs:
        results: A DataFrame with columns for user_id and number_of_answers. Follows similar logic to bigquery_experts_results shown above.
    """
    q = f"""
        SELECT a.owner_user_id AS user_id, COUNT(1) AS number_of_answers
        FROM `bigquery-public-data.stackoverflow.posts_questions` AS q
        INNER JOIN `bigquery-public-data.stackoverflow.posts_answers` AS a
            ON q.id = a.parent_Id
        WHERE q.tags like '%{topic}%'
        GROUP BY a.owner_user_id
        """

    # TODO https://github.com/mallory-jpg/mallory-jpg.github.io/issues/1 
    # Set up the query (a real service would have good error handling for
    # queries that scan too much data)
    safe_config = bigquery.QueryJobConfig(maximum_bytes_billed=10**10)
    my_query_job = client.query(q, job_config=safe_config)

    # API request - run the query, and return a pandas DataFrame
    results = my_query_job.to_dataframe()

    return results
