from database import Database
from table import Table
from PIL import Image
import pickle
import os
from graphviz import Digraph


#Συνάρτηση κατακερματισμού hash_division

def hash_division(value , size: int):
    if type(value) is int:
        index = value%size
    else:
        index = ascii_sum_convert(str(value))%size
    return index

#Συνάρτηση κατακερματισμού hash_folding

def hash_folding(value , size: int , pairnum:int = 2):
    strlist = []
    new = properformat(value , pairnum)
    n = 0
    pairlength = int(len(new)/pairnum)
    for i in range(0 , len(new) , pairlength):
        part = new[i:pairlength+i]
        strlist.append(part)
    for i in strlist:
        n+=int(i)    
    index = n%size         
    return index

#Μετατρέπει ένα string σε αριθμούς και τους προσθέτει μεταξύ τους

def ascii_sum_convert(string:str):
    value = 0
    for c in string:
        value+=(ord(c))
    return value

#Μετατρέπει ένα string σε αριθμούς και τους συνδυάζει μεταξύ τους

def ascii_str_convert(string:str):
    value = ''
    for c in string:
        value+=str(ord(c))
    return value

#Μετατρέπει ένα string σε string του οποίου οι χαρακτήρες διαιρούνται επακριβώς σε n = painum τμήματα

def properformat(value , pairnum: int):
    if type(value) is int:
        strvalue = str(value)
    else:
        strvalue = ascii_str_convert(str(value))
    if len(strvalue)%pairnum is 0:
        n=0
    else:
        n = pairnum - len(strvalue)%pairnum
    extra = ''
    for i in range(n):
        extra+='0'
    new = extra+strvalue
    return new


#Δέχεται ως είσοδο ένα πίνακα και το όνομα μιας στήλης και επιστρέφει το index αυτηνής στον πίνακα αυτόν

def findcolumnindex(table ,column_name):
        for i in range (len(table.column_names)):
            if table.column_names[i] == column_name:
                return i

#Επιστρέφει το path του pkl αρχείου απο το folder της βάσης

def pkl_file_path(dbname , filename):
    dbfoldername = dbname+'_db'
    dbfolderpath = 'dbdata/'+dbfoldername
    filepath = dbfolderpath+'/'+filename+'.pkl'
    return filepath

#Επιστρέφει το file path στο οποίο θα αποθηκευτεί το αρχείο κατακερματισμού

def hash_file_path(db_name,table_name,column_name):
    file_name = table_name+'_hash_on_'+column_name
    path = pkl_file_path(db_name,file_name)
    return path

#Επιστρέφει τον πίνακα ως object

def get_table(db_name,table_name):
    db = Database(db_name , load=True)
    table = db.select(table_name ,'*',return_object=True)
    return table

#Έλεγχος ύπαρξης πίνακα

def tb_exists(db_name,table_name):
    filepath = pkl_file_path(db_name,table_name)
    print(filepath)
    exists = os.path.exists(filepath)
    return exists


class Hash:

   #Επιστρέφει ως Hash object (αν υπάρχει) τον αποθηκευμένο πίνακα κατακερματισμού

    @staticmethod
    def existing(db_name:str , table_name:str , column_name:str):
        hpath = hash_file_path(db_name,table_name,column_name)

        #Έλεγχος ορθότητας παραμέτρων

        if not os.path.exists(hpath):
            print('Αποτυχία: Δεν βρέθηκε αρχείο κατακερματισμού για τον πίνακα :'+table_name+' της βάσης: '+db_name+' στην στήλη: '+column_name)
            return None
            
        pickle_input = open(hpath,'rb')
        obj = pickle.load(pickle_input)
        return obj      

    #Constructor της κλάσης Hash

    def __init__(self , db_name:str , table_name:str , column_name:str , size:int , method ,save=True):

        #Έλεγχος ορθότητας παραμέτρων db_name και table_name

        if not tb_exists(db_name , table_name):
            raise Exception('Αποτυχία: Δεν βρέθηκε πίνακας :'+table_name+' της βάσης: '+db_name+' στην στήλη: '+column_name)

        #Ορίζουμε τα attributes του αντικειμένου Hash
        
        self.db = Database(db_name , load=True)
        self.table = get_table(db_name , table_name)  
        self.column_index = findcolumnindex(self.table,column_name)
        self.size = size
        self.method = method
        self.hash_list=[[None]]*size

        if column_name not in self.table.column_names:
            raise Exception('Αποτυχία: Δεν βρέθηκε στήλη: '+column_name+' στον πίνακα: '+table_name+' της βάσης: '+db_name)


        #Δημιουργία πίνακα

        for record in self.table.data:
            value=record[self.column_index]
            hash_index = method(value,size)
            if self.hash_list[hash_index] == [None]:
                self.hash_list[hash_index] = [record]
            else:
                self.hash_list[hash_index].append(record)

        #Αποθήκευση πίνακα

        if save:    

            hpath = hash_file_path(db_name,table_name,column_name)

            print('Επιτυχής δημιουργία hash file για τον πίνακα :'+table_name+' της βάσης: '+db_name+' στην στήλη: '+column_name)
            print('Hash File Path :'+hpath)
            with open(hpath,'wb') as fl:
                pickle.dump(self,fl)
                  
    #Αναζήτηση ευρετηρίου

    def search(self, value):
        index = self.method(value , self.size)
        block = self.hash_list[index]
        for record in block:
            if record != None:
                record_value = record[self.column_index]
                if record_value == value:
                    return record
        return None


    #Συνάρτηση Hash Join

    @staticmethod
    def join(db_name: str, tableA_name: str, tableB_name: str, column_name: str, size: int, method, final_name: str):

        #Έλεγχος ορθότητας παραμέτρων

        if tb_exists(db_name , final_name):
            raise Exception('Υπάρχει ήδη πίνακας με το όνομα: '+final_name+' της βάσης: '+ db_name)
        if tb_exists(tableA_name , final_name):
            raise Exception('Δε βρέθηκε πίνακας: '+tableA_name+' στην βάση: '+ db_name)
        if tb_exists(tableB_name , final_name):
            raise Exception('Δε βρέθηκε πίνακας: '+tableB_name+' στην βάση: '+ db_name)
        

        tableA = get_table(db_name, tableA_name)
        tableB = get_table(db_name, tableB_name)

        #Οι αντίστοιχες θέσης της στήλης πάνω στην οποία γίνεται η σύνδεση

        column_index_A=findcolumnindex(tableA,column_name)
        column_index_B=findcolumnindex(tableB,column_name)

        #Η θέση της κοινής στήλης που θα διαγραφεί απο τον τελικό πίνακα

        index_tbr = len(tableA.column_names) + column_index_B

        column_names = tableA.column_names + tableB.column_names
        column_types = tableA.column_types + tableB.column_types

        column_names.pop(index_tbr)
        column_types.pop(index_tbr)

        #Δημιουργία (προσωρινών) πινάκων pπάνω στην κονή στήλη

        hash1 = Hash(db_name,tableA_name,column_name,size,method , False)
        hash2 = Hash(db_name, tableB_name, column_name,size,method , False)

        # Δημιουργία νέου πίνακα
        db = Database(db_name, load=True)

        db.create_table(final_name , column_names , column_types)

        #Διαδικασία σύνδεσης

        for b_idx, blockA in enumerate(hash1.hash_list):
            blockB = hash2.hash_list[b_idx]
            if blockA != [None] and blockB != [None]:
                for resultA in blockA:
                    for resultB in blockB:
                        if resultA[column_index_A] == resultB[column_index_B]:
                            resultA.extend(resultB)
                            resultA.pop(index_tbr)
                            db.insert(final_name , resultA)
        final = get_table(db_name,final_name)
        return final


    #Visualization του hash_table

    def visualize(self):
        h = self.hash_list
        g = Digraph()
        for i, results in enumerate(h):            
            g.node(str(i),shape='square')
            for r in h[i]:
                if r is None:
                    g.node(str(r)+str(i),label='Empty',shape='rectangle')
                    g.edge(str(i),str(r)+str(i))
                else:
                    g.node(str(r),shape='rectangle')
                    g.edge(str(i),str(r))        
        g.view()
 