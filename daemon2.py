from DAL import DAL


dal = DAL()

def check_existence(table, column, field):
    result = dal.select(table, column, where="name='%s'"%field, fetch=True)

    if len(result) > 0:
        print("gbg")
    else:
        dal.insert(table, 'name', field)

    id = dal.select(table, column, where="name='%s'"%field, fetch=False)
    id = id[0]

    return id

def insert_data(data):
    for i in data:
        old_id = i['old_id']
        summarized_content = i['summarized_content']
        title = i['title']
        date = i['date']
        category_id = i['category_id']
        location_id = i['location_id']
        site_id =i['site_id']
        dal.connection_open()
        category_id = check_existence("category", "*", category_id)
        site_id= check_existence("site", "*", site_id)
        location_id=check_existence("location", "*", location_id)
        news_massiv = (title, date, summarized_content, category_id, location_id, site_id, old_id)
        column_names = 'title, date, summarized_content, category_id, location_id, site_id, old_id'
        values = (
            news_massiv[0].replace('\'', '-'),
            news_massiv[1],
            news_massiv[2].replace('\'', '-'),
            news_massiv[3],
            news_massiv[4],
            news_massiv[5],
            news_massiv[6]
        )
        dal.insert("similar_news", column_names, *values)
        dal.connection_close()
