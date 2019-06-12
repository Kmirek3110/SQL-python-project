import psycopg2
import json
import sys
import bcrypt
inFile = sys.argv[1]

git_exit = """{"status": "OK"}"""
bad_exit = """{"status": "ERROR"}"""

connected = 0
good=0
inita = 0


#Sprawdza czy x podane z wejscia jest tym samym co z hashowanym hasłem w bazie
def check_password(x,y):
    return bcrypt.checkpw(x,y)
    
#Funkcja służaca do sprawdzenia czy w tabeli allid nie znajduje się id które chcemy dodać
def checker_allid(id):
    cursor.execute("""SELECT * from AllId;""")
    x=cursor.fetchall()
    for i in x:
        if id in i:
            return False
    return True

#Obsługuje wszystkie zapytania  
def which_query(line):
    global good,inita,connected
    good=1
    if 'open' in line:
        if line['open']['password'] == 'qwerty':
            connected =1
            if line['open']['login'] == 'init':
                init()
                inita=1
        else:
            good = 0
    if connected == 1 and inita ==1:
        if 'leader' in line:
            member(line['leader'],1)
    elif connected ==1 and inita ==0:
        if 'protest' in line:
            action(line['protest'],"protest")
        if 'support' in line:
            action(line['support'],"support")
        if "downvote" in line:
            vote(line["downvote"],"downvote")
        if 'upvote' in line:
            vote(line['upvote'],'upvote')
        if 'actions' in line:
            actions(line['actions'])
        if 'projects' in line:
            projects(line["projects"])
        if 'votes' in line:
            votes(line['votes']) 
        if 'trolls' in line:
            trolls(line['trolls']) 
        if 'leader' in line:
            good=0   
    else :
        good =0
#Wypisuje trolli (funkcja api)
def trolls(query):

    global good
    good=2
   
    q=(""" Select * from (Select member, SUM(votes_for) as a, SUM(votes_against) as b, """+
    "Case WHEN " + str(query["timestamp"]) + """ - Members.timestamp < 31536000 Then 'true' else 'false' END """+
    """from Actions join Members ON(Actions.member_id=member)  Group by member) as foo  where b > a Order by b-a DESC, member ASC;""") 
    cursor.execute(q)
    x=cursor.fetchall()
    print("""{"status": "OK",
    "data": """,x, "}")
#Sprawdza czy member jest liderem
def checker_isleader(id):
    cursor.execute("Select member,is_leader from Members where member = " + str(id) +" and "
    + "is_leader = 1" +";")
    x=cursor.fetchall()
    if x == []:
        return False
    return True
#Wypisuje akcje (funkcja api)
def actions(query):
    global good
    cursor.execute("""SELECT member,password,timestamp,is_leader FROM Members ;""")
    x=cursor.fetchall()   
    mem_leader_exist =0
    for i in x :
        if query['member'] in i : # czy istnieje
            if checker_isleader(query["member"]):
                if check_password(query["password"],i[1]): # dobre haslo  
                    if query['timestamp'] - i[2] < 31536000 : # czy zamrozona
                        mem_leader_exist=1

    if mem_leader_exist ==1 :
        good=2
        if 'type' in query and 'project' in query and 'authority' in query:
            
            q=("SELECT Actions.id,type,project_id,authorityid,votes_for,votes_against from Actions join Projects " +
             "ON(Actions.project_id = Projects.id) where type = " + "'"+str(query['type']) +"' and "+
             " project_id = " + str(query["project"])+ " and authorityid =" +str(query["authority"])+" ORDER By Actions.Id ; ")
            
            cursor.execute(q)
            x=cursor.fetchall()
            print("""{"status": "OK",
    "data": """,x, "}")
        elif 'project' in query and 'type' in query:
            
            q=("SELECT Actions.id,type,project_id,authorityid,votes_for,votes_against from Actions join Projects " +
             "ON(Actions.project_id = Projects.id) where project_id = " + str(query["project"]) + 
             " and type = " + "'"+str(query['type']) +"'"+" ORDER By Actions.Id ;")
            cursor.execute(q)
            x=cursor.fetchall()
            print("""{"status": "OK",
    "data": """,x, "}")
        elif 'project' in query and 'authority' in query:
            q=("SELECT Actions.id,type,project_id,authorityid,votes_for,votes_against from Actions join Projects " +
             "ON(Actions.project_id = Projects.id) where project_id = " + str(query["project"]) + 
             " and authority = " + str(query['authority']) +" ORDER By Actions.Id ;")
            cursor.execute(q)
            x=cursor.fetchall()
            print("""{"status": "OK",
    "data": """,x, "}")
        elif 'type' in query and 'authority' in query :
            q=("SELECT Actions.id,type,project_id,authorityid,votes_for,votes_against from Actions join Projects " +
             "ON(Actions.project_id = Projects.id) where authority = " + str(query["authority"]) + 
             " and type = " + "'"+str(query['type']) +"'"+" ORDER By Actions.Id ;")
            cursor.execute(q)
            x=cursor.fetchall()
            print("""{"status": "OK",
    "data": """,x, "}")
        elif 'project' in query:
            q=("SELECT Actions.id,type,project_id,authorityid,votes_for,votes_against from Actions join Projects " +
             "ON(Actions.project_id = Projects.id) where project_id = " + str(query["project"]) + 
            " ORDER By Actions.Id ;")
            cursor.execute(q)
            x=cursor.fetchall()
            print("""{"status": "OK",
    "data": """,x, "}")
        elif 'type' in query:
            q=("SELECT Actions.id,type,project_id,authorityid,votes_for,votes_against from Actions join Projects " +
             "ON(Actions.project_id = Projects.id) where " + 
             " type = " + "'"+str(query['type']) +"'"+" ORDER By Actions.Id ;")
            cursor.execute(q)
            x=cursor.fetchall()
            print("""{"status": "OK",
    "data": """,x, "}")
        elif 'authority' in query:
            q=("SELECT Actions.id,type,project_id,authorityid,votes_for,votes_against from Actions join Projects " +
             "ON(Actions.project_id = Projects.id) where authorityid =" +str(query["authority"]) + "  ORDER By Actions.Id;")
            cursor.execute(q)
            x=cursor.fetchall()
            print("""{"status": "OK",
    "data": """,x, "}")
        else:
            
            q=("SELECT Actions.id,type,project_id,authorityid,votes_for,votes_against from Actions join Projects " +
             "ON(Actions.project_id = Projects.id) ORDER By Actions.Id ;")
            cursor.execute(q)
            x=cursor.fetchall()
            print("""{"status": "OK",
    "data": """,x, "}")  
        update_timestamp(query)
    else:
        good = 0
#Wypisuje projekty(funkcja api)
def projects(query):
    global good
    cursor.execute("""SELECT member,password,timestamp,is_leader FROM Members ;""")
    x=cursor.fetchall()   
    mem_leader_exist =0
    
    for i in x :
        if query['member'] in i : # czy istnieje
            if checker_isleader(query["member"]): # czy to jest lider
                if check_password(query["password"],i[1]): # dobre haslo               
                    if query['timestamp'] - i[2] < 31536000 : # czy zamrozony                   
                        mem_leader_exist=1
    if mem_leader_exist ==1 :
        good =2
        if 'authority' in query:
            q=("SELECT * from  Projects " +
                " where authorityid =" +str(query["authority"]) + "  ORDER By id asc ;")
        else :
            q=("SELECT * from  Projects   ORDER By id asc;")
            
        cursor.execute(q)
        x=cursor.fetchall()
        print("""{"status": "OK",
    "data": """,x, "}")
        
        update_timestamp(query)
    else :
        good = 0

#Dodawanie projektu do bazy danych
def project_add(query):
    global good
    if checker_allid(query["authority"]):
        all_id(query["authority"])
    if checker_allid(query["project"]):
        q= (""" INSERT INTO Projects (id,authorityid) VAlues ( """ +  str(query["project"]) +", "
         + str(query["authority"])+ ');')  
        cursor.execute(q)
        all_id(query["project"])
    else:
        good=0
#Dodawania głosów
def up_or_downvote(query,t):

    global good
    cursor.execute("Select member_id,action_id from Votes WHERE member_id = "+str(query['member']) +
    " and " + "action_id = " +str(query['action']) +";")
    x=cursor.fetchall()
    if x == []:
        good =1
        if t == "downvote":
            q=(""" Update Actions SET votes_against = votes_against+1 """
            + "Where id = " + str(query['action']) + ";")   
            e=(""" Insert into votes (action_id,member_id,voted_against) Values ( """
            + str(query['action']) +", "+str(query['member'])+", " + "1" + ");")    
        else: 
            q=(""" Update Actions SET votes_for = votes_for+1 """
            + "Where id = " + str(query['action']) + ";" )
            e=(""" Insert into votes (action_id,member_id,voted_for) Values ( """
            + str(query['action']) +", "+str(query['member'])+", " + "1" + ");") 
        
        cursor.execute(q)
        cursor.execute(e)
        update_timestamp(query)  
    else:
        good = 0

#Wypisuje głosy funkcja (api)
def votes(query):
    global good
    cursor.execute("""SELECT member,password,timestamp,is_leader FROM Members ;""")
    x=cursor.fetchall()   
    mem_leader_exist =0
    
    for i in x :
        if query['member'] in i : # czy istnieje
            if checker_isleader(query["member"]):
               if check_password(query["password"],i[1]): # dobre haslo                 
                    if query['timestamp'] - i[2] < 31536000 : # czy zamrozona                    
                        mem_leader_exist=1
    if mem_leader_exist:
        good=2 
       
        if 'action' in query:
            
            #tutaj mam problem z zapytaniem
            q=("SELECT member,SUM(coalesce(voted_for,0)) as a,SUM(coalesce(voted_against,0)) as b "
            + " from Members left JOIN Votes on (Members.member=Votes.member_id) "+ 
            "where action_id = " +str(query["action"])+" GROUP BY member Order by member;")     
                
            cursor.execute(q)
            x=cursor.fetchall()

        elif 'project' in query:
            
            q=("SELECT member,SUM(coalesce(voted_for,0)) as a,SUM(coalesce(voted_against,0)) as b  "
            +"from Members LEFT JOIN Votes on (Members.member=Votes.member_id) "
             +" Join   Actions On(Actions.id=Votes.action_id)"+
            " where project_id = " +str(query["project"])+
            " GROUP BY member Order by member;")
            cursor.execute(q)
            x=cursor.fetchall()
            
        else:
           
            q=("SELECT member,SUM(coalesce(voted_for,0)) as a,SUM(coalesce(voted_against,0)) as b "
            +"from Members LEFT JOIN Votes on (Members.member=Votes.member_id)  Group by member Order by member;")
            cursor.execute(q)
            x=cursor.fetchall()
            
        print("""{"status": "OK",
        "data": """,x, "}")
        update_timestamp(query)
    else:
        good =0

#Funkcja która wstępnie przeprowadza dodania głosu robi wszystkie checki
def vote(query,t):
    global good
    cursor.execute("""SELECT member,password,timestamp FROM Members ;""")
    x=cursor.fetchall()   
   
    error=0
    mem_exist=0
    
    for i in x :
        if query['member'] in i : # czy istnieje
            mem_exist=1
            if check_password(query["password"],i[1]): # dobre haslo  
                if query['timestamp'] - i[2] < 31536000 : # czy zamrozona
                    error=0
                else:
                    error=1
            else:
                error=1 
    if error == 0:
        if mem_exist == 0:  #nie istnieje osoba ktora chce zrobic akcje wiec ją tworzymy
            member(query,0)  # 0 bo nie lider
        cursor.execute("""SELECT id from Actions ;""")

        x=cursor.fetchall()
        action_exist = 0 
        for i in x:
            if query['action'] in i:
                up_or_downvote(query,t)
                action_exist = 1     
        #jeśli nie istnieje to go tworzymy
        if action_exist == 0:
            good =0
    else:
        good =0

# Updatowanie timestampu 
def update_timestamp(query):
    cursor.execute("""Update Members Set timestamp = """ + str(query["timestamp"]) + " WHERE member = " + str(query["member"]) +";")
#Dodawanie akcji protestu lub supportu do bazy danych
def add_protest_or_support(query,t):
    global good
    #dodajemy akcje  supportu lub protestu
    if checker_allid(query["action"]):
        q=("""INSERT INTO Actions (id,project_id,member_id,type,timestamp) Values
            ( """ + str(query['action']) + ', '+ str(query["project"])+", "
             + str(query["member"]) +", "+"'"+t+"'"+ ", " +str(query["timestamp"]) +
             ");") #Insertujemy akcje po sprawdzeniu wszystkiego
        cursor.execute(q)
        all_id(query["action"])
        update_timestamp(query)
    else :
        good=0
#Funkcja która wstępnie przeprowadza dodania akcji robi wszystkie checki
def action(query,t):
    global good
    cursor.execute("""SELECT member,password,timestamp FROM Members ;""")
    x=cursor.fetchall()   
    error=0
    mem_exist=0
    for i in x :
        if query['member'] in i : # czy istnieje
            mem_exist=1
            if check_password(query["password"],i[1]): # dobre haslo  
                if query['timestamp'] - i[2] < 31536000 : # czy zamrozona    
                    error=0
                else:
                    error=1
            else:
                error=1               
    if error == 0:
        if mem_exist == 0:  #nie istnieje osoba ktora chce zrobic akcje wiec ją tworzymy
            member(query,0)  # 0 bo nie lider

        
        cursor.execute("""SELECT * From Projects ;""")
        x=cursor.fetchall()
        
        project_exist = 0 
        for i in x:
            if query['project'] in i:
                add_protest_or_support(query,t)
                project_exist = 1     

        #jeśli nie istnieje to go tworzymy
        if project_exist == 0:
            project_add(query)
            add_protest_or_support(query,t)
    else:
        good = 0
                                                    
#Dodawanie id do tabelu allid
def all_id(id):
    cursor.execute("""INSERT INTO AllId (id) VALUES ( """ + str (id) + """ ) ;""")

#Dodawanie membera do bazy danych
def member(dict,n):
    global good
    x=bcrypt.hashpw(str(dict['password']),bcrypt.gensalt())
    if checker_allid(dict["member"]):
        query= ("""INSERT INTO Members (timestamp,password,member,is_leader) VALUES ( """ + "'" + str((dict['timestamp']))
        + "'" + """, """ +  "'" + x + "'" + """, """ + str(dict['member']) 
        + """ , """ + str(n) + """); """)
        cursor.execute(query)
        all_id(dict["member"])
    else:
        good =0

# Zapytania SQL budujące naszę baze
def init():  
    f=open("projekt.sql")
    full_sql=f.read()
    sql_commands = full_sql.replace('\n', '').split(';')[:-1] 
    for x in sql_commands:
     q=str(x)+ " ;"
     cursor.execute(q)
    
inputowa=[]
#Czytanie z plików wejścia i jego obsługi
with open(inFile,'r') as f:
    for line in f:
        inputowa.append(json.loads(line)) 

connection=0
if 'open' in inputowa[0]:
    if 'init' == inputowa[0]["open"]['login']:
        connect_str = "dbname='student' user='init' host='localhost' " + \
            "password='qwerty' "
        connection =1

    elif 'app' == inputowa[0]["open"]['login']:
        if  inputowa[0]["open"]['password'] == 'qwerty':
            connect_str = "dbname='student' user='" + str(inputowa[0]['open']['login']) + "' host='localhost' " + \
            "password='qwerty' "
            connection =1
if connection:      
    conn=psycopg2.connect(connect_str)
    cursor = conn.cursor()
for line in inputowa:
    if connection:
        which_query(line) 
        if good == 1:
            print(git_exit)
        elif good == 0:
            print(bad_exit)
    else :
        print(bad_exit)
        
if connection:
    conn.commit()
    cursor.close()
    conn.close()