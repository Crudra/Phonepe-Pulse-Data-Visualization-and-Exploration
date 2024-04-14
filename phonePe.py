import os
import pandas as pd
import json
import mysql.connector

import mysql.connector 
import streamlit as st
import PIL 
from PIL import Image
from streamlit_option_menu import option_menu
import plotly.express as px
import matplotlib.pyplot as plt
import requests
import base64  


# creating DB connection
mydb = mysql.connector.connect(
  host='127.0.0.1',
  user="root",
  password="nayagam57",
  database = "phonepe",
  auth_plugin = 'mysql_native_password'
)
cursor = mydb.cursor()

#This is to extract the Aggregated transaction data's to create a dataframe and insert into SQL

def aggregated_transaction():
    path="C:/Users/RUDRA/project/capstone2/phonepe/pulse/data/aggregated/transaction/country/india/state/"
    Agg_state_list=os.listdir(path)
    clm={'State':[], 'Year':[],'Quarter':[],'Transaction_type':[], 'Transaction_count':[], 'Transaction_amount':[]}

    for i in Agg_state_list:
        p_i=path+i+"/"
        Agg_yr=os.listdir(p_i)
        for j in Agg_yr:
            p_j=p_i+j+"/"
            Agg_yr_list=os.listdir(p_j)
            for k in Agg_yr_list:
                p_k=p_j+k
                Data=open(p_k,'r')
                D=json.load(Data)
                for z in D['data']['transactionData']:
                    Name=z['name']
                    count=z['paymentInstruments'][0]['count']
                    amount=z['paymentInstruments'][0]['amount']
                    clm['Transaction_type'].append(Name)
                    clm['Transaction_count'].append(count)
                    clm['Transaction_amount'].append(amount)
                    clm['State'].append(i)
                    clm['Year'].append(j)
                    clm['Quarter'].append(int(k.strip('.json')))
    #Succesfully created a dataframe
    Agg_Trans=pd.DataFrame(clm)
    Agg_Trans["State"] = Agg_Trans["State"].str.replace("-"," ")
    Agg_Trans["State"] = Agg_Trans["State"].str.title()

    for index,row in Agg_Trans.iterrows():
        insert_query = '''INSERT INTO aggregated_transaction (states, years, quarter, transaction_type, transaction_count, transaction_amount)
                                                            values(%s,%s,%s,%s,%s,%s)'''
        values = (row["State"],
                row["Year"],
                row["Quarter"],
                row["Transaction_type"],
                row["Transaction_count"],
                row["Transaction_amount"]
                )
        cursor.execute(insert_query,values)
        mydb.commit()

#This is to extract the Aggregated user data's to create a dataframe and insert into SQL
        
def aggregated_user():
    path = "C:/Users/RUDRA/project/capstone2/phonepe/pulse/data/aggregated/user/country/india/state/"      
    Agg_user_list = os.listdir(path)
    clm = {"States":[], "Years":[], "Quarter":[], "Brands":[],"Transaction_count":[], "Percentage":[]}
    for i in Agg_user_list:
        p_i = path+i+"/"
        Agg_yr = os.listdir(p_i) 
        for j in Agg_yr:
            p_j = p_i+j+"/"
            Agg_yr_list = os.listdir(p_j)     
            for k in Agg_yr_list:
                p_k = p_j+k
                Data = open(p_k,"r")
                D = json.load(Data)
                try:
                    for z in D["data"]["usersByDevice"]:
                        brand = z["brand"]
                        count = z["count"]
                        percentage = z["percentage"]
                        clm["Brands"].append(brand)
                        clm["Transaction_count"].append(count)
                        clm["Percentage"].append(percentage)
                        clm["States"].append(i)
                        clm["Years"].append(j)
                        clm["Quarter"].append(int(k.strip(".json")))          
                except:
                    pass

    Agg_User = pd.DataFrame(clm)
    Agg_User["States"] = Agg_User["States"].str.replace("-"," ")
    Agg_User["States"] = Agg_User["States"].str.title()

    for index,row in Agg_User.iterrows():
        insert_query = '''INSERT INTO aggregated_user (states, years, quarter, brands, transaction_count, percentage)
                                                        values(%s,%s,%s,%s,%s,%s)'''
        values = (row["States"],
                row["Years"],
                row["Quarter"],
                row["Brands"],
                row["Transaction_count"],
                row["Percentage"])
        cursor.execute(insert_query,values)
        mydb.commit()

#This is to extract the map transaction data's to create a dataframe and insert into SQL

def map_transaction():
    path ="C:/Users/RUDRA/project/capstone2/phonepe/pulse/data/map/transaction/hover/country/india/state/"
    map_tran_list = os.listdir(path)
    columns = {"States":[], "Years":[], "Quarter":[],"District":[], "Transaction_count":[],"Transaction_amount":[]}
    for i in map_tran_list:
        p_i = path+i+"/"
        map_year_list = os.listdir(p_i)     
        for j in map_year_list:
            p_j = p_i+j+"/"
            map_file_list = os.listdir(p_j)
            for k in map_file_list:
                p_k = p_j+k
                data = open(p_k,"r")
                D = json.load(data)
                for z in D['data']["hoverDataList"]:
                    name = z["name"]
                    count = z["metric"][0]["count"]
                    amount = z["metric"][0]["amount"]
                    columns["District"].append(name)
                    columns["Transaction_count"].append(count)
                    columns["Transaction_amount"].append(amount)
                    columns["States"].append(i)
                    columns["Years"].append(j)
                    columns["Quarter"].append(int(k.strip(".json")))
    map_trans = pd.DataFrame(columns)
    map_trans["States"] = map_trans["States"].str.replace("-"," ")
    map_trans["States"] = map_trans["States"].str.title()
    
    for index,row in map_trans.iterrows():
            insert_query = '''
                INSERT INTO map_transaction (states, years, quarter, district, transaction_count, transaction_amount)
                VALUES (%s, %s, %s, %s, %s, %s)

            '''
            values = (
                row['States'],
                row['Years'],
                row['Quarter'],
                row['District'],
                row['Transaction_count'],
                row['Transaction_amount']
            )
            cursor.execute(insert_query,values)
            mydb.commit() 

#This is to extract the map user data's to create a dataframe and insert into SQL

def map_user():
    path ="C:/Users/RUDRA/project/capstone2/phonepe/pulse/data/map/user/hover/country/india/state/"

    map_user_list = os.listdir(path)
    columns = {"States":[], "Years":[], "Quarter":[], "Districts":[], "RegisteredUser":[], "AppOpens":[]}
    for i in map_user_list:
        p_i = path+i+"/"
        map_year_list = os.listdir(p_i)
        
        for j in map_year_list:
            p_j = p_i+j+"/"
            map_file_list = os.listdir(p_j)
            
            for k in map_file_list:
                p_k = p_j+k
                data = open(p_k,"r")
                D = json.load(data)

                for z in D["data"]["hoverData"].items():
                    district = z[0]
                    registereduser = z[1]["registeredUsers"]
                    appopens = z[1]["appOpens"]
                    columns["Districts"].append(district)
                    columns["RegisteredUser"].append(registereduser)
                    columns["AppOpens"].append(appopens)
                    columns["States"].append(i)
                    columns["Years"].append(j)
                    columns["Quarter"].append(int(k.strip(".json")))

    map_user_df = pd.DataFrame(columns)

    map_user_df["States"] = map_user_df["States"].str.replace("-"," ")
    map_user_df["States"] = map_user_df["States"].str.title()

    for index,row in map_user_df.iterrows():
        insert_query4 = '''INSERT INTO map_user (states, years, quarter, districts, registeredUser, appOpens)
                            values(%s,%s,%s,%s,%s,%s)'''
        values = (row["States"],
                row["Years"],
                row["Quarter"],
                row["Districts"],
                row["RegisteredUser"],
                row["AppOpens"])
        cursor.execute(insert_query4,values)
        mydb.commit()

#This is to extract the top transaction data's to create a dataframe and insert into SQL

def top_transaction():
    path = "C:/Users/RUDRA/project/capstone2/phonepe/pulse/data/top/transaction/country/india/state/"
    top_tran_list = os.listdir(path)

    columns = {"States":[], "Years":[], "Quarter":[], "Pincodes":[], "Transaction_count":[], "Transaction_amount":[]}

    for i in top_tran_list:
        p_i = path+i+"/"
        top_year_list = os.listdir(p_i)
        
        for j in top_year_list:
            p_j = p_i+j+"/"
            top_file_list = os.listdir(p_j)
            
            for k in top_file_list:
                p_k = p_j+k
                data = open(p_k,"r")
                D = json.load(data)

                for z in D["data"]["pincodes"]:
                    entityName = z["entityName"]
                    count = z["metric"]["count"]
                    amount = z["metric"]["amount"]
                    columns["Pincodes"].append(entityName)
                    columns["Transaction_count"].append(count)
                    columns["Transaction_amount"].append(amount)
                    columns["States"].append(i)
                    columns["Years"].append(j)
                    columns["Quarter"].append(int(k.strip(".json")))

    top_transaction_df = pd.DataFrame(columns)

    top_transaction_df["States"] = top_transaction_df["States"].str.replace("-"," ")
    top_transaction_df["States"] = top_transaction_df["States"].str.title()

    for index,row in top_transaction_df.iterrows():
        insert_query = '''INSERT INTO top_transaction (states, years, quarter, pincodes, transaction_count, transaction_amount)
                                                        values(%s,%s,%s,%s,%s,%s)'''
        values = (row["States"],
                row["Years"],
                row["Quarter"],
                row["Pincodes"],
                row["Transaction_count"],
                row["Transaction_amount"])
        cursor.execute(insert_query,values)
        mydb.commit()

#This is to extract the top user data's to create a dataframe and insert into SQL
def top_user():
    path = "C:/Users/RUDRA/project/capstone2/phonepe/pulse/data/top/user/country/india/state/"
    top_user_list = os.listdir(path)
    columns = {"States":[], "Years":[], "Quarter":[], "Pincodes":[], "RegisteredUser":[]}
    
    for i in top_user_list:
        p_i = path+i+"/"
        top_year_list = os.listdir(p_i)
        for j in top_year_list:
            p_j = p_i+j+"/"
            top_file_list = os.listdir(p_j)
            for k in top_file_list:
                p_k = p_j+k
                data = open(p_k,"r")
                D = json.load(data)
                for z in D["data"]["pincodes"]:
                    name = z["name"]
                    columns["Pincodes"].append(name)
                    columns["RegisteredUser"].append(z["registeredUsers"])
                    columns["States"].append(i)
                    columns["Years"].append(j)
                    columns["Quarter"].append(int(k.strip(".json")))
    
    top_user_df = pd.DataFrame(columns)
    
    top_user_df["States"] = top_user_df["States"].str.replace("-"," ")
    top_user_df["States"] = top_user_df["States"].str.title()

    for index,row in top_user_df.iterrows():
        insert_query = '''INSERT INTO top_user (states, years, quarter, pincodes, registeredUser)
                                                values(%s,%s,%s,%s,%s)'''
        values = (row["States"],
                  row["Years"],
                  row["Quarter"],
                  row["Pincodes"],
                  row["RegisteredUser"])
        cursor.execute(insert_query,values)
        mydb.commit()


#Function to display the sidebar background
def sidebar_bg(side_bg):

   side_bg_ext = 'png'

   st.markdown(
      f"""
      <style>
      [data-testid="stSidebar"] > div:first-child {{
          background: url(data:image/{side_bg_ext};base64,{base64.b64encode(open(side_bg, "rb").read()).decode()});
      }}
      </style>
      """,

      
      unsafe_allow_html=True,
      )
   
side_bg = r'C:\Users\RUDRA\project\capstone2\phonepe\p4.png'
sidebar_bg(side_bg)

with st.sidebar:
    st.caption("")
    st.caption("")
    st.caption("")
    st.caption("")
    st.caption("")

#with st.headbar:
SELECT = option_menu(
    menu_title = None,
    options = ["Home","Top Charts","Geo Graph","Explore State"],
    icons =["house","bar-chart","map","search"],
    default_index=0,
    orientation="horizontal",
    styles={"container": {"padding": "0!important", "background-color": "white","size":"cover", "width": "100"},
        "icon": {"color": "black", "font-size": "20px"},
            
        "nav-link": {"font-size": "20px", "text-align": "center", "margin": "-2px", "--hover-color": "#6F36AD"},
        "nav-link-selected": {"background-color": "#6F36AD"}})

if SELECT == "Home":
    st.subheader("To offer every Indian equal opportunity to accelerate their progress by unlocking the flow of money and access to services")
    st.subheader("")
    st.image(Image.open(r'C:\Users\RUDRA\project\capstone2\phonepe\p1.png'),width = 700)


        # MENU 2 - TOP CHARTS
if SELECT == "Top Charts":
    st.markdown("## :violet[Top Charts]")
    Type = st.selectbox("**Type**", ("Transactions", "Users"))
    #colum1,colum2= st.columns([1,1.8],gap="medium")
    #with colum1:
    Year = st.slider("**Year**", min_value=2018, max_value=2022)
    Quarter = st.slider("Quarter", min_value=1, max_value=4)
     
# Top Charts - TRANSACTIONS    
    if Type == "Transactions":
        if Year == 2022 and Quarter in [2,3,4]:
             st.markdown("No Data to Display for the selected Quarter in 2022")
        else:
            st.markdown("### :violet[State]")
            cursor.execute(f"select states, sum(Transaction_count) as Total_Transactions_Count, sum(Transaction_amount) as Total from aggregated_transaction where years = {Year} and quarter = {Quarter} group by states order by Total desc limit 10")
            df = pd.DataFrame(cursor.fetchall(), columns=['State', 'Transactions Count','Total Amount'])
            fig = px.pie(df, values='Total Amount',
                             names='State',
                             title='Top 10 State',
                             color_discrete_sequence=px.colors.sequential.thermal,
                             hover_data=['Transactions Count'],
                             labels={'Transactions Count':'Transactions Count'})

            fig.update_traces(textposition='inside', textinfo='percent+label')
            st.plotly_chart(fig,use_container_width=True)
            
        
            st.markdown("### :violet[District]")
            cursor.execute(f"select district , sum(transaction_count) as Total_Count, sum(transaction_amount) as Total from map_transaction where years = {Year} and quarter = {Quarter} group by district order by Total desc limit 10")
            df = pd.DataFrame(cursor.fetchall(), columns=['District', 'Transactions Count','Total Amount'])

            fig = px.pie(df, values='Total Amount',
                             names='District',
                             title='Top 10 District',
                             color_discrete_sequence=px.colors.sequential.thermal,
                             hover_data=['Transactions Count'],
                             labels={'Transactions Count':'Transactions Count'})

            fig.update_traces(textposition='inside', textinfo='percent+label')
            st.plotly_chart(fig,use_container_width=True)

            st.markdown("### :violet[Pincode]")
            cursor.execute(f"select pincodes , sum(transaction_count) as Total_Count, sum(transaction_amount) as Total from top_transaction where years = {Year} and quarter = {Quarter} group by pincodes order by Total desc limit 10")
            df = pd.DataFrame(cursor.fetchall(), columns=['Pincode', 'Transactions Count','Total Amount'])

            fig = px.pie(df, values='Total Amount',
                             names='Pincode',
                             title='Top 10',
                             color_discrete_sequence=px.colors.sequential.thermal,
                             hover_data=['Transactions Count'],
                             labels={'Transactions Count':'Transactions Count'})

            fig.update_traces(textposition='inside', textinfo='percent+label')
            st.plotly_chart(fig,use_container_width=True)

                     
# Top Charts - USERS          
    if Type == "Users":    
        if Year == 2022 and Quarter in [2,3,4]:
             st.markdown("No Data to Display for the selected Quarter in 2022")
        else:
               
            st.markdown("### :violet[State]")
            cursor.execute(f"select states, sum(Registereduser) as Total_Users, sum(AppOpens) as Total_Appopens from map_user where years = {Year} and quarter = {Quarter} group by states order by Total_Users desc limit 10")
            df = pd.DataFrame(cursor.fetchall(), columns=['State', 'Total Users','Total Appopens'])

            fig = px.bar(df,
                         title='Top 10 State',
                         x="Total Users",
                         y="State",
                         orientation='h',
                         color='Total Users',
                         color_continuous_scale=px.colors.sequential.thermal)
            st.plotly_chart(fig,use_container_width=True)


            st.markdown("### :violet[District]")
            cursor.execute(f"select districts, sum(RegisteredUser) as Total_Users, sum(AppOpens) as Total_Appopens from map_user where years = {Year} and quarter = {Quarter} group by districts order by Total_Users desc limit 10")
            df = pd.DataFrame(cursor.fetchall(), columns=['District', 'Total Users','Total Appopens'])
            df['Total Users'] = df['Total Users'].astype(float)
            fig = px.bar(df,
                         title='Top 10 District',
                         x="Total Users",
                         y="District",
                         orientation='h',
                         color='Total Users',
                         color_continuous_scale=px.colors.sequential.thermal)
            st.plotly_chart(fig,use_container_width=True)

            st.markdown("### :violet[Brands]")
            cursor.execute(f"select brands, sum(transaction_count) as Total_Count, avg(percentage)*100 as Avg_Percentage from aggregated_user where years = {Year} and quarter = {Quarter} group by brands order by Total_Count desc limit 10")
            df = pd.DataFrame(cursor.fetchall(), columns=['Brand', 'Total_Users','Avg_Percentage'])
            fig = px.bar(df,
                            title='Top 10 Brands',
                            x="Total_Users",
                            y="Brand",
                            orientation='h',
                            color='Avg_Percentage',
                            color_continuous_scale=px.colors.sequential.thermal)
            st.plotly_chart(fig,use_container_width=True)
              
if SELECT == "Geo Graph":
    Year = st.slider("**Year**", min_value=2018, max_value=2022)
    Quarter = st.slider("Quarter", min_value=1, max_value=4)
    Type = st.selectbox("**Type**", ("Transactions", "Users"))
    col1,col2 = st.columns(2)
    
# EXPLORE DATA - TRANSACTIONS
    if Type == "Transactions":
        # Overall State Data - TRANSACTIONS AMOUNT - INDIA MAP 
        
        st.markdown("## :violet[All India state wise - Transactions Amount]")
        cursor.execute(f"select states, sum(transaction_count) as Total_Transactions, sum(transaction_amount) as Total_amount from map_transaction where years = {Year} and quarter = {Quarter} group by states order by states")
        df1 = pd.DataFrame(cursor.fetchall(),columns= ['State', 'Total Transactions', 'Total Amount'])
        
        fig = px.choropleth(df1,geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                    featureidkey='properties.ST_NM',
                    locations='State',
                    color='Total Amount',
                    color_continuous_scale='thermal')

        fig.update_geos(fitbounds="locations", visible=True)
        st.plotly_chart(fig,use_container_width=True)          
        
        # Overall State Data - TRANSACTIONS COUNT - INDIA MAP
            
        st.markdown("## :violet[All India state wise - Transactions Count]")
        cursor.execute(f"select states, sum(transaction_count) as Total_Transactions, sum(transaction_amount) as Total_amount from map_transaction where years = {Year} and quarter = {Quarter} group by states order by states")
        df1 = pd.DataFrame(cursor.fetchall(),columns= ['State', 'Total Transactions', 'Total Amount'])
        df1['Total Transactions'] = df1['Total Transactions'].astype(int)

        fig = px.choropleth(df1,geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                    featureidkey='properties.ST_NM',
                    locations='State',
                    color='Total Transactions',
                    color_continuous_scale='thermal')

        fig.update_geos(fitbounds="locations", visible=False)
        st.plotly_chart(fig,use_container_width=True)

    if Type == 'Users':

        # Overall State Data - TRANSACTIONS AMOUNT - INDIA MAP 
        
        st.markdown("## :violet[All India state wise - Total Users]")
        cursor.execute(f"select states, sum(Registereduser) as Total_Users, sum(AppOpens) as Total_Appopens from map_user where years = {Year} and quarter = {Quarter} group by states order by states")
        df = pd.DataFrame(cursor.fetchall(), columns=['State', 'Total Users','Total Appopens'])        
        fig = px.choropleth(df,geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                    featureidkey='properties.ST_NM',
                    locations='State',
                    color='Total Users',
                    color_continuous_scale='thermal')

        fig.update_geos(fitbounds="locations", visible=True)
        st.plotly_chart(fig,use_container_width=True)          
        
        # Overall State Data - TRANSACTIONS COUNT - INDIA MAP
            
        st.markdown("## :violet[All India state wise - Transactions Count]")
        cursor.execute(f"select states, sum(transaction_count) as Total_Transactions, sum(transaction_amount) as Total_amount from map_transaction where years = {Year} and quarter = {Quarter} group by states order by states")
        df1 = pd.DataFrame(cursor.fetchall(),columns= ['State', 'Total_Transactions', 'Total_amount'])
        df1.Total_Transactions = df1.Total_Transactions.astype(int)

        fig = px.choropleth(df1,geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                    featureidkey='properties.ST_NM',
                    locations='State',
                    color='Total_Transactions',
                    color_continuous_scale='thermal')

        fig.update_geos(fitbounds="locations", visible=False)
        st.plotly_chart(fig,use_container_width=True)


if SELECT == "Explore State":
        
    st.markdown("## :violet[Top Charts]")
    Type = st.selectbox("**Type**", ("Transactions", "Users"))
    #colum1,colum2= st.columns([1,1.8],gap="medium")
    #with colum1:
    Year = st.slider("**Year**", min_value=2018, max_value=2022)
    Quarter = st.slider("Quarter", min_value=1, max_value=4)

    # EXPLORE State - TRANSACTIONS
    if Type == "Transactions":
        
        cursor.execute(f"select states from map_transaction where years = {Year} and quarter = {Quarter} group by states order by states ")
        df1 = pd.DataFrame(cursor.fetchall(),columns= ['State'])

        select_statesdropdown = df1["State"]

        states= st.selectbox("Select The State for analysis", select_statesdropdown)
        

        cursor.execute(f"select states, district , sum(transaction_count) as Total_Transactions, sum(transaction_amount) as Total_amount from map_transaction where years = {Year} and quarter = {Quarter} and states = '{states}' group by states, district,years,quarter order by states,district")
        df1 = pd.DataFrame(cursor.fetchall(),columns= ['State','District','Total Transactions', 'Total_amount'])
        
        fig = px.bar(df1,
                     title=states,
                     x="District",
                     y="Total Transactions",
                     orientation='v',
                     color='District',
                     color_continuous_scale=px.colors.sequential.thermal)
        st.plotly_chart(fig,use_container_width=True)


        select_districtdropdown = df1["District"]


        district= st.selectbox("Select The District for analysis", select_districtdropdown)
        cursor.execute(f"select quarter ,transaction_amount as Total_amount from map_transaction where years = {Year}  and district = '{district}' and states = '{states}'")
        df1 = pd.DataFrame(cursor.fetchall(),columns= ['Quarter','Total Amount'])
        col1,col2= st.columns([1,1.8],gap="medium")
        with col1:
            st.write(df1)

        with col2:
            fig_line_1= px.line(df1, x= "Quarter", 
                                y= "Total Amount", 
                                hover_data= "Total Amount",
                                title= f"{district.upper()} - Transaction Amount in each Quarter",
                                width= 500, markers= True)
            st.plotly_chart(fig_line_1)

    if Type == "Users":
            
            cursor.execute(f"select states from map_user where years = {Year} and quarter = {Quarter} group by states order by states ")
            df1 = pd.DataFrame(cursor.fetchall(),columns= ['State'])

            select_statesdropdown = df1["State"]

            states= st.selectbox("Select The State for analysis", select_statesdropdown)
            

            cursor.execute(f"select states, districts , registeredUser from map_user where years = {Year} and quarter = {Quarter} and states = '{states}'")
            df1 = pd.DataFrame(cursor.fetchall(),columns= ['State','District','Registered Users'])
            
            fig = px.bar(df1,
                        title=states,
                        x="District",
                        y="Registered Users",
                        orientation='v',
                        color='District',
                        color_continuous_scale=px.colors.sequential.thermal)
            st.plotly_chart(fig,use_container_width=True)


            select_districtdropdown = df1["District"]


            district= st.selectbox("Select The District for analysis", select_districtdropdown)
            cursor.execute(f"select quarter ,appOpens from map_user where years = {Year}  and districts = '{district}' and states = '{states}'")
            df1 = pd.DataFrame(cursor.fetchall(),columns= ['Quarter','App Opens'])
            col1,col2= st.columns([1,1.8],gap="medium")
            with col1:
                st.write(df1)

            with col2:
                fig_line_1= px.line(df1, x= "Quarter", 
                                    y= "App Opens", 
                                    hover_data= "App Opens",
                                    title= f"{district.upper()} - App Opens in each Quarter",
                                    width= 500, markers= True)
                st.plotly_chart(fig_line_1)

            