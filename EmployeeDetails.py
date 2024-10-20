from elasticsearch import Elasticsearch,helpers
import pandas as pd 


# Connect to Elasticsearch
es = Elasticsearch("http://localhost:9200")

# 1. Function to create a collection
def createCollection(p_collection_name):
    if not es.indices.exists(index=p_collection_name):
        es.indices.create(index=p_collection_name)
        print(f"Created collection: {p_collection_name}")
    else:
        print(f"Collection {p_collection_name} already exists")

# 2. Function to index data excluding a column
def indexData(p_collection_name, p_exclude_column):
    # Load the employee dataset
    df = pd.read_csv('employee_sample_data.csv')
    
    # Exclude the specified column
    if p_exclude_column in df.columns:
        df = df.drop(columns=[p_exclude_column])
    
    # Convert DataFrame rows to JSON and index them into Elasticsearch
    actions = [
        {
            "_index": p_collection_name,
            "_source": row.to_dict(),
        }
        for _, row in df.iterrows()
    ]
    helpers.bulk(es, actions)
    print(f"Indexed data into collection: {p_collection_name}")

# 3. Function to search by a column
def searchByColumn(p_collection_name, p_column_name, p_column_value):
    query = {
        "query": {
            "match": {p_column_name: p_column_value}
        }
    }
    result = es.search(index=p_collection_name, body=query)
    print(f"Search results for {p_column_name} = {p_column_value}:")
    for hit in result['hits']['hits']:
        print(hit['_source'])

# 4. Function to get the employee count
def getEmpCount(p_collection_name):
    result = es.count(index=p_collection_name)
    print(f"Total employees in {p_collection_name}: {result['count']}")

# 5. Function to delete an employee by ID
def delEmpById(p_collection_name, p_employee_id):
    es.delete(index=p_collection_name, id=p_employee_id)
    print(f"Deleted employee with ID: {p_employee_id}")

# 6. Function to get employee count grouped by department
def getDepFacet(p_collection_name):
    query = {
        "aggs": {
            "departments": {
                "terms": {"field": "Department.keyword"}
            }
        }
    }
    result = es.search(index=p_collection_name, body=query, size=0)
    print("Employee count by department:")
    for bucket in result['aggregations']['departments']['buckets']:
        print(f"{bucket['key']}: {bucket['doc_count']}")

# Execution order
v_nameCollection = 'Hash_<Your Name>'  # Replace with your name
v_phoneCollection = 'Hash_<Your Phone last four digits>'  # Replace with your phone's last 4 digits

# 1. Create collections
createCollection(v_nameCollection)
createCollection(v_phoneCollection)

# 2. Get employee count before indexing
getEmpCount(v_nameCollection)

# 3. Index data into collections
indexData('Hash_JohnDoe', 'Department')
indexData(v_phoneCollection, 'Gender')

# 4. Delete employee by ID
delEmpById(v_nameCollection, 'E02003')

# 5. Get employee count after deletion
getEmpCount(v_nameCollection)

# 6. Search for employees by department and gender
searchByColumn(v_nameCollection, 'Department', 'IT')
searchByColumn(v_nameCollection, 'Gender', 'Male')
searchByColumn(v_phoneCollection, 'Department', 'IT')

# 7. Get department facet
getDepFacet(v_nameCollection)
getDepFacet(v_phoneCollection)