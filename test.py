from database import Database

db = Database('unipi', load=False)

db.create_table('teachers', ['id','name', 'subject', 'salary'], [int,str,str,int] ,primary_key='id')

db.create_table('subjects', ['subject','difficulty', 'semester'], [str,str,int] ,primary_key='subject')

db.insert('teachers', ['0','Theodoridis','SDBD','1200'])
db.insert('teachers', ['1','Birbou','Allhlepidrash','700'])
db.insert('teachers', ['2','Patsakis','Python','1000'])
db.insert('teachers', ['3','Alepis','OOP','900'])
db.insert('teachers', ['4','Sapounakis','Analysh','1100'])
db.insert('teachers', ['5','Douligeris','Oures','850'])
db.insert('teachers', ['6','Metaxiotis','Pliroforiaka','700'])
db.insert('teachers', ['7','Apostolou','AI','1150'])
db.insert('teachers', ['8','Pikrakis','Metaglwtistes','900'])
db.insert('teachers', ['9','Tsikouras','Algebra','950'])


db.insert('subjects', ['Allhlepidrash','Easy','5'])
db.insert('subjects', ['SDBD','Medium','5'])
db.insert('subjects', ['Analysh','Hard','1'])
db.insert('subjects', ['Algebra','Medium','1'])
db.insert('subjects', ['OOP','Easy','4'])
db.insert('subjects', ['Metaglwtistes','Easy','3'])






