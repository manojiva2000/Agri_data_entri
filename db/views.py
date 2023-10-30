import json
import re
import sqlite3
import time
import pandas as pd
from django.http import HttpResponse, JsonResponse
from django.shortcuts import render
from django.db import connection,utils
from django.views.decorators.csrf import csrf_exempt
from pandas import *
from django.db import transaction
from django.core.cache import cache
import threading
from django.dispatch import Signal
from tqdm import tqdm

data_insertion_progress = Signal()
# Create your views here.
def home(request):
    return render(request,'index.html')
@csrf_exempt
def get_tablenames(request):
    
    # executing our sql query
    with connection.cursor() as cursor:
        cursor.execute( """SELECT name FROM sqlite_master  
  WHERE type='table';""")
        table_list = (cursor.fetchall())
    print(table_list)
        
    return JsonResponse({'table_list':table_list})
@csrf_exempt
def create_tables(request):
    data = request.POST
    print(data)
    data_dict = dict(data)
    table_name = data_dict['table_name'][0]
    del data_dict['table_name']
    print(data_dict)
    query_string=""
    header_list =[]
    for x in data_dict:
        if char_check(x)=='break':
            msg = x + " has special character"
            return JsonResponse({'msg':msg})
        else:
            header_list.append(x)
            query_string1 = x +" "+ str(data_dict[x][0]+'(155)')+','
            query_string+=query_string1
    print(header_list)
    print(query_string[:-1])
    try:
        with connection.cursor() as cursor: 
            print('oo')
            if table_name!='':
                print('noooooo')
                cursor.execute(f" CREATE TABLE {table_name}({query_string[:-1]});")
                msg ="success"
            else:
                msg ="Please enter Table name"
            
        return JsonResponse({'msg':msg,'table_name':table_name,'header_list':header_list})
    except  utils.DatabaseError as e:
        print(e)
        if 'Index' in data_dict:
            msg = "Index is a reserved keyword"  
        else:
            msg = str(e)
        return JsonResponse({'msg':msg,'table_name':table_name,'header_list':header_list})


#special character checking
def char_check(string):
    regex = re.compile('[@!#$%^&*()<>?/\|}{~:]')
    if(regex.search(string) == None):
        print("String is accepted")
        stat ="go"
         
    else:
        print("String is not accepted.")
        stat = 'break'
    return stat
    


#query_retrieving
@csrf_exempt
def query_execute(request):
    data = request.POST
    # print(data)
    sql_query = data['query']
    with connection.cursor() as cursor:
        try:
            cursor.execute(""""""+
        sql_query+"""
    """)
            data_col = cursor.fetchall()
            headers = [description[0] for description in cursor.description]
        

        # Use regular expressions to extract the table name
            match = re.search(r'FROM\s+([^\s]+)', sql_query, re.I)

            if match:
                table_name = match.group(1)
            print(table_name)
        

            # print(table_name)
            # print(cursor.fetchall())
           # print([description[0] for description in cursor.description])
            df = pd.DataFrame(data_col, columns=headers).to_dict(orient='records')
            print(headers)

            # row = cursor.fetchall()
            # print(row)
            return JsonResponse({'headers':headers,'df':df,'table_name':table_name,'msg':'success'},safe=False,)
        except Exception as e:
            return JsonResponse({'msg':'fail','message':str(e)})

@csrf_exempt
def file_read(request):
    str_data = request.POST
    print(str_data)
    if str_data['type'] =='read':
        data = request.FILES
        # table_name = str_data['table_name']
        print(str_data)
        csv_file = data['csv']
        csv_data = read_csv(csv_file)
        print(type(csv_data))
        csv_col = csv_data.to_dict(orient='records')
        print(type(csv_col))
        header_list = []
        for i in csv_data :
            # print(i)
            header_list.append(i)
        # print(header_list)
        return JsonResponse({'list':header_list,'data':csv_col})
    else:
        data = request.FILES
        table_name = str_data['table_name']
        # table_name = 'Train3'

        # table_name = str_data['table_name']
        with connection.cursor() as cursor:
            cursor.execute(f"SELECT * FROM {table_name} LIMIT 1");
            print(cursor.description)
            column_list = [col[0] for col in cursor.description]
        print(column_list)
        csv_file = data['csv']
        csv_data = read_csv(csv_file)
        print(type(csv_data))
        csv_col = csv_data.to_dict(orient='records')
        print(type(csv_col))
        header_list = []
        for i in csv_data :
            print(i)
            header_list.append(i)
        print(header_list)
        return JsonResponse({'list':header_list,'csv_data':csv_col,'column_list':column_list})

    
@csrf_exempt
def insert_data(request):
    data = request.POST
    #print(data)
    table_name = data['table_name']
    print(table_name)
    header_list = json.loads(data['header_list'])
    query_string ='insert into '+table_name+'('
    print(query_string)
    csv_data = json.loads(data['csv_data'])
    print(type(csv_data))
    data_csv = pd.DataFrame(csv_data)
    header_list = []
    data_dict = {}
    for i in range(0,len(csv_data)):
        # print(len(csv_data))
        col_list = data_csv.columns.tolist()
        # print(list(data_csv.iloc[0].values))
    for list in col_list:
        query_string+=list+','
    thread = threading.Thread(target=insert_fn,args=(data_csv,col_list,header_list,query_string))
    thread.start()
            # cursor.execute( """"""+query_string[:-1]+""") values("""+query_data+""");""")
        # for j in data_csv[i]:
        #     print(j)
        # data_dict[i]=data_csv[i].to_list
    # print(header_list)
    # print(data_dict)
    # with connection.cursor() as cursor:
    #     cursor.execute( """DROP TABLE """+table_name+""";""")
    return JsonResponse({'msg':'Data insertion started'})

def insert_fn(data_csv,col_list,columns,query_string):
   total_rows = len(data_csv)
   
   with transaction.atomic():
       with connection.cursor() as cursor:
            pbar = tqdm(total=total_rows)

            for i in range(total_rows):
                data_type = data_csv.iloc[i].values.tolist()
                for list in col_list:
                    if list not in columns:
                        index = col_list.index(list)
                        data_type[index]='null'
                        query_data = str(data_type)[1:-1]
                    else:
                        query_data = str(data_type)[1:-1]
                # print(query_data)

                # if len(col_list) == len(columns):
                #     data_type = data_csv.iloc[i].values.tolist()
                #     query_data = str(data_type)[1:-1]
                # else:
                #     data_type = data_csv.iloc[i].values.tolist()
                #     query_data = str(data_type)[1:-1]
                #     for i in range(0,len(col_list)):
                #         query_data
                # print(query_string)
                # cursor.execute(f'INSERT INTO {table_name}({query_string[:-1]})values({query_data});')
                try:
                    
                    cursor.execute( """"""+query_string[:-1]+""") values("""+query_data+""");""")
                    progress = int((i) * 100 / total_rows)
                
                    pbar.update(1)
                    cache.set('data_insertion_progress', progress)
                    delay_microseconds = 15 / 1_000_000
                    time.sleep(delay_microseconds)
                except Exception as e:
                    print(e)
                    progress = str(e)
                    cache.set('data_insertion_progress', progress)
            for i in range(100,102):   
                print(i)
                cache.set('data_insertion_progress', i)
                time.sleep(1)
                # Send progress to the signal     
                

            pbar.close()
       
@csrf_exempt
def get_function_status(request):
    progress = cache.get('data_insertion_progress')
    print(progress)
    if progress is not None:
        return JsonResponse({'progress': progress})

    return JsonResponse({'progress': progress})

@csrf_exempt
def delete_table(request):
    data = request.POST
    print(data)
    table_name = data['table_name']
    print(table_name)
    # executing our sql query
    with connection.cursor() as cursor:
        try:
            cursor.execute( """DROP TABLE """+table_name+""";""")
            
            msg = 'Table Deleted'
            c=0
             
            cursor.execute( """SELECT name FROM sqlite_master  
  WHERE type='table';""")
            table_list = (cursor.fetchall())
            print(table_list)
        except Exception as e:
            msg = str(e)
            c=1
    
        
    return JsonResponse({'message':msg,'table_list':table_list,'status':c})

@csrf_exempt
def download_file(request):
    data = request.POST
    #print(data)
    table_name = data['table_name']
    csv_data = json.loads(data['csv_data'])
    print(type(csv_data))
    data_csv = pd.DataFrame(csv_data)
    csv__data = data_csv.to_csv(index=False)

    # Create an HTTP response with the CSV content and appropriate headers
    response = HttpResponse(csv__data, content_type='text/csv')
    response['Content-Disposition'] = 'attachment; filename="data_.csv"'

    return response

@csrf_exempt
def insert_only(request):
    data = request.POST
    print(data['columns'])
    columns = json.loads(data['columns'])
    table_name = data['table_name']
    query_string ='insert into '+table_name+'('
    csv_data = json.loads(data['csv_data'])
    print(type(csv_data))
    data_csv = pd.DataFrame(csv_data)
    col_list =[]
    for i in range(0,len(csv_data)):
        # print(len(csv_data))
        col_list = data_csv.columns.tolist()
        # print(list(data_csv.iloc[0].values))
    print(col_list)
    print(type(columns))
    for i in col_list:
        query_string+=i+','
    # for i in col_list:
    #     if i not in columns:
    #         del data_csv[i]
    print(query_string)

    thread = threading.Thread(target=insert_fn,args=(data_csv,col_list,columns,query_string))
    thread.start()
            # cursor.execute( """"""+query_string[:-1]+""") values("""+query_data+""");""")
        # for j in data_csv[i]:
        #     print(j)
        # data_dict[i]=data_csv[i].to_list
    # print(header_list)
    # print(data_dict)
    # with connection.cursor() as cursor:
    #     cursor.execute( """DROP TABLE """+table_name+""";""")
    return JsonResponse({'msg':'Data insertion started'})




    
    


