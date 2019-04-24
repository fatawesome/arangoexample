from pyArango.connection import *


def db_connection():
    conn = Connection(username='root', password='')
    return conn["Example"]


def add_document(key, name):
    db = db_connection()
    doc = db["People"].createDocument()
    doc["_key"] = key
    doc["name"] = name
    print('Adding the document')
    print(doc)
    doc.save()


def add_more_documents():
    db = db_connection()
    students = [('Oscar', 'Wilde', 3.5), ('Thomas', 'Hobbes', 3.2),
                ('Mark', 'Twain', 3.0), ('Kate', 'Chopin', 3.8), ('Fyodor', 'Dostoevsky', 3.1),
                ('Jane', 'Austen', 3.4), ('Mary', 'Wollstonecraft', 3.7), ('Percy', 'Shelley', 3.5),
                ('William', 'Faulkner', 3.8), ('Charlotte', 'Bronte', 3.0)]
    for (first, last, gpa) in students:
        doc = db["People"].createDocument()
        doc['name'] = "%s %s" % (first, last)
        doc['gpa'] = gpa
        doc['year'] = 2017
        doc._key = ''.join([first, last]).lower()
        doc.save()


def report_gpa(document):
    print("Student: %s" % document['name'])
    print("GPA:     %s" % document['gpa'])


def top_scores(col, gpa):
    print("Top Soring Students:")
    for student in col.fetchAll():
        if student['gpa'] >= gpa:
            print("- %s" % student['name'])


add_more_documents()

print("\n--- GPA for Jane Austen")
report_gpa(db_connection()["People"]["janeausten"])

print("\n--- Top Scores ---")
top_scores(db_connection()["People"], 3.5)
print("------")

print("\n--- AQL select")
db = db_connection()
aql = "FOR x IN People RETURN x._key"
res = db.AQLQuery(aql, rawResults=True)
print(res)

print('\n--- AQL insert')
doc = {'_key': 'denisdiderot', 'name': 'Denis Diderot', 'gpa': 3.7}
bind = {"doc": doc}
aql = "INSERT @doc INTO People LET newDoc = NEW RETURN newDoc"
res = db.AQLQuery(aql, bindVars=bind)
print(res)

print('\n--- AQL filter')
aql = "FOR x IN People FILTER x.gpa == 3.0 RETURN x"
res = db.AQLQuery(aql, rawResult=False)
print(res)

