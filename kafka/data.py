import csv

def get_customer():
    file=open('kafka/Mall_Customers.csv')
    data=csv.reader(file)
    data_list=list(data)
    return data_list

if __name__=="__main__":
    get_customer()