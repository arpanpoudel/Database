from Database import DB
import sys



#connection.createDB("Fortune500",fields_size)
# connection.open("Fortune500")
# connection.readDB(DBsize,rec_size)
#connection.readRecord(501)
#print(connection.record)
# connection.findRecord("arpada")
#print(connection.found_loc)
#connection.updateRecord(["ALaPHABET","29","MOUNTAIN VIEW","CA", "94043","72053"])
#connection.deleteRecord("ALPHABET")
#connection.addRecord(["arpada","12","Jhapa","CA", "4043","12"])
# connection.linearSearch("arpada")
# connection.readRecord(499)
# print(connection.record)

def captureInput():
    """
    Function to capture the selection from the user
    
    """ 
    while True:
        try:
            selection=int(input("Enter the selection for operation: "))
            if selection in range(1,10):
                break
            else:
                print("Please enter value in range of 1-9")
                
        except:
            print("Enter a integer value")
    return selection

#checks if user want to restart from the beginning or conitinue
def reset(connection):
    ans = str(input('Continue with the DB? (y/n) : '))
    print('\n')
    if ans == 'y':
        return True
    else:
        connection.close()
        main()
#function for recursion
def recursion(connection,field_size):
    take=captureInput()
    if take!=9:
        operation(take,connection,field_size)
    else:
        print("Exit")
        sys.exit 
           
def operation(selection,connection,field_size):
    """
    Performs the specified operation

    Args:
        selection (int): selection to do the operation
    """
    # create database
    if selection == 1:
        filename=input("Enter the filename you want to create a database: ")
        if connection.createDB(filename,field_size=field_size)==True:
                main()
        else:
            print(f"{filename} file does not exist. Try again..")
            main()
    
    # open database
    if selection == 2:
        
        filename=input("Enter the filename you want to open a database: ")
        if connection.open(filename):
            rec_size = sum(list(field_size))
            DBsize = 10
            connection.readDB(DBsize,rec_size)
            if reset(connection)== True:
                recursion(connection,field_size)
                
        else:
            main()
            
    # close database
    if selection == 3:
        if connection.close():
            print(f"[INFO] {connection.file_name} closed successfully")
            main()    
        else:
            print("[Error] Database closed failed, Please open Database to close the database.")
            main()

    # Display record for the given input name
    if selection == 4:
        if connection.isOpen():
            key=input("Enter the name of the company to search: ")
            if connection.findRecord(key)==True:
                print("Data found at location",connection.found_loc)
                print(f"Name:{connection.record['Name']}, Rank:{connection.record['Rank']}, City:{connection.record['City']}, State:{connection.record['State']}, Zip:{connection.record['Zip']}, Employees: {connection.record['Employees']}")
                recursion(connection,field_size)
            else:
                print(f"[Error] No data found for {key}")
                recursion(connection,field_size)
        else:
            print(f"[Error] Database is not open. Please open")
            main()
    #Update record for the given name
    if selection == 5:
        if connection.isOpen():
            key=input("Enter the name of the company to Update: ")
            flag=connection.findRecord(key)
            if flag==True:
                rank=input("Enter the updated rank: ")
                city=input("Enter the updated city: ")
                state=input("Enter the updated state: ")
                Zip=input("Enter the updated Zip: ")
                employee=input("Enter the updated employees: ")
                if connection.updateRecord([key,rank,city,state,Zip,employee], flag)== True:
                    print(f"[INFO] Database updated for {key}")
                    
            else:
                print(f"[Error] No data found for {key}")
            recursion(connection,field_size)
        else:
            print(f"[Error] Database is not open. Please open")
            main()
        
    # create a report 
    if selection==6:
        if connection.isOpen():
            connection.createReport()
            recursion(connection,field_size)
        else:
            print(f"[Error] Database is not open. Please open")
            main()
    
    # add records to the database
    if selection==7:
        if connection.isOpen():
            name=input("Enter the name of the company: ")
            rank=input("Enter the rank: ")
            city=input("Enter the city: ")
            state=input("Enter the state: ")
            Zip=input("Enter the Zip: ")
            employee=input("Enter the employees: ")
            
            if connection.addRecord([name,rank,city,state,Zip,employee])==True:
                print(f"[INFO] Data added for {name}")
            else:
                print(f"[Error] Add failed.")
                
            recursion(connection,field_size)
        
        else:
            print(f"[Error] Database is not open. Please open")
            main()
    
    #delete records from the database
    if selection==8:
        if connection.isOpen():
            key=input("Enter the name of the company to delete: ")
            if connection.deleteRecord(key)== True:
                print(f"[INFO] Database deleted for {key}")
            else:
                print(f"[INFO] No record found for {key}")
            recursion(connection,field_size)
        else:
            print(f"[Error] Database is not open. Please open")
            main()
            
    #exit the program
    if selection==9:
        print('Exit Program')
        sys.exit            
                
    
    
        
def display():
    """
    shows the available options to the user
    """ 
       # Display
    print('_______________________________________')
    print('_______________________________________')
    print('_____    1. Create a new Database _____')
    print('_____    2. Open Database         _____')
    print('_____    3. Close Database        _____')
    print('_____    4. Display record        _____')
    print('_____    5. Update record         _____')
    print('_____    6. Create Report         _____')
    print('_____    7. Add a record          _____')
    print('_____    8. Delete a record       _____')
    print('_____    9. Quit                  _____')
    print('_______________________________________')
    print('_______________________________________')
    print('\n')
       
def main():
    
     # field length definitions
    name_len,rank_len,city_len,state_len,zip_len,employees_len=(50,4,20,3,6,8)

    fields_size = (name_len,rank_len,city_len,state_len,zip_len,employees_len)
    

    #instance of Database
    connection=DB()
    
    #capturee input from user
    display()
    selection=captureInput()
    operation(selection=selection, connection=connection,field_size=fields_size)
    
    
if __name__=='__main__':
    main()
    