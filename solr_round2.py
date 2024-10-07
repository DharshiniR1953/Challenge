import requests
import csv
import subprocess
import os
import chardet

SOLR_URL = 'http://localhost:8983/solr'

def createCore(core_name):
    print(f"Creating core '{core_name}'...")
    try:
        response = requests.get(f'{SOLR_URL}/admin/cores?action=CREATE&name={core_name}&configSet=_default&wt=json')
        response.raise_for_status()
        print(f"Core '{core_name}' created successfully.")
    except requests.exceptions.HTTPError as err:
        print(f"Error creating core '{core_name}': {err}")

def indexData(core_name, exclude_column):
    print(f"Indexing data into core '{core_name}' excluding column '{exclude_column}'...")
    try:
        with open('Employee Sample Data.csv', 'rb') as f:
            result = chardet.detect(f.read())
        encoding = result['encoding']
        print(f"Detected encoding: {encoding}")

        with open('Employee Sample Data.csv', 'r', encoding=encoding) as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                if exclude_column in row:
                    del row[exclude_column]
                print(f"Indexing row: {row}") 
                response = requests.post(f'{SOLR_URL}/{core_name}/update?commit=true', json=[row], timeout=30)

                response.raise_for_status()
        print(f"Data indexed successfully into '{core_name}'.")
    except Exception as e:
        print(f"Error indexing data into '{core_name}': {e}")

def searchByColumn(core_name, column_name, column_value):
    print(f"Searching in core '{core_name}' where '{column_name}' = '{column_value}'...")
    try:
        response = requests.get(f'{SOLR_URL}/{core_name}/select?q={column_name}:{column_value}&wt=json')
        response.raise_for_status()
        results = response.json()['response']['docs']
        if results:
            for doc in results:
                print(doc)
        else:
            print(f"No results found for {column_name} = {column_value} in '{core_name}'.")
    except Exception as e:
        print(f"Error searching in '{core_name}': {e}")

def delEmpById(core_name, employee_id):
    print(f"Deleting employee '{employee_id}' from core '{core_name}'...")
    try:
        response = requests.post(f'{SOLR_URL}/{core_name}/update?commit=true', json={"delete": {"id": employee_id}})
        response.raise_for_status()
        print(f"Employee '{employee_id}' deleted successfully.")
    except Exception as e:
        print(f"Error deleting employee '{employee_id}' from '{core_name}': {e}")

def getEmpCount(core_name):
    print(f"Getting employee count in core '{core_name}'...")
    try:
        response = requests.get(f'{SOLR_URL}/{core_name}/select?q=*:*&rows=0&wt=json')
        response.raise_for_status()
        count = response.json()['response']['numFound']
        print(f"Employee count in '{core_name}': {count}")
        return count
    except Exception as e:
        print(f"Error getting employee count in '{core_name}': {e}")
        return 0

def getDepFacet(core_name):
    print(f"Retrieving department facets from core '{core_name}'...")
    try:
        response = requests.get(f'{SOLR_URL}/{core_name}/select?q=*:*&facet=true&facet.field=Department&wt=json')
        response.raise_for_status()
        facet_counts = response.json()['facet_counts']['facet_fields']['Department']
        print(f"Department facets in '{core_name}': {facet_counts}")
    except Exception as e:
        print(f"Error retrieving department facets from '{core_name}': {e}")

if __name__ == "__main__":
    v_nameCollection = 'dharshini'
    v_phoneCollection = '2578'

    createCore(v_nameCollection)
    createCore(v_phoneCollection)

    getEmpCount(v_nameCollection)

    indexData(v_nameCollection, 'Department')
    indexData(v_phoneCollection, 'Gender')

    delEmpById(v_nameCollection, 'E02003')

    getEmpCount(v_nameCollection)

    searchByColumn(v_nameCollection, 'Department', 'IT')
    searchByColumn(v_nameCollection, 'Gender', 'Male')
    searchByColumn(v_phoneCollection, 'Department', 'IT')
    getDepFacet(v_nameCollection)
    getDepFacet(v_phoneCollection)
