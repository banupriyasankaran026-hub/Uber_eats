import pandas as pd
import sqlite3
import streamlit as st

df1=pd.read_csv("C:\\Users\\banup\\Desktop\\Uber_Eats project1\\Uber_Eats_data.csv")

df2=pd.read_json("C:\\Users\\banup\\Desktop\\Uber_Eats project1\\orders.json")

#print(df1.head())
#print(df1.shape)
#print(df1.info())
#print(df1.isnull().sum())

df1=df1.drop_duplicates()
#print(df1.shape)

df1['rate']=df1['rate'].str.split("/").str[0]
#print(df1['rate'])
df1['rate']=pd.to_numeric(df1['rate'],errors='coerce')
df1['rate']=df1['rate'].fillna(df1['rate'].mean())
#print(df1['rate'].isnull().sum())

df1=df1.rename(columns={'rate':'rating','approx_cost(for two people)':'approx_cost'})

df1['approx_cost']=pd.to_numeric(df1['approx_cost'],errors='coerce')
df1['approx_cost']=df1['approx_cost'].fillna(df1['approx_cost'].median())
#print(df1['approx_cost'].isnull().sum())

def pricing_segment(cost):
    if cost>=800:
        return "High"
    elif cost>=400:
        return "Medium"
    else:
        return "Low"

df1['pricing_segment']=df1['approx_cost'].apply(pricing_segment)

#print(df1.head())
def rating_category(rate):
    if rate>=4.5:
        return "Excellent"
    elif rate>=3.5:
        return "Good"
    elif rate>=2.5:
        return "Poor"
    else:
        return "Very Poor"

df1['rating_category']=df1['rating'].apply(rating_category)
#print(df1.head())

conn=sqlite3.connect("Uber_Eats.db")
cursor=conn.cursor()

cursor.execute("""
    CREATE TABLE IF NOT EXISTS restaurants(
        Name VARCHAR(150),
        Online_order VARCHAR(10),
        Book_table VARCHAR(10),
        Rating DECIMAL(2,1),
        Votes INT,
        Phone VARCHAR(50),
        Location VARCHAR(100),
        Rest_type VARCHAR(150),
        Dish_liked VARCHAR(300),
        Cusines VARCHAR(200),
        Approx_cost INT,
        Listed_type VARCHAR(50),
        Listed_city VARCHAR(50),  
        Pricing_segment VARCHAR(50),
        Rating_category VARCHAR(50)                
        )
    """)

data=df1.values.tolist()

query="""INSERT INTO restaurants(
            Name,Online_order,Book_table,Rating,Votes,Phone,Location,
            Rest_type,Dish_liked,Cusines,Approx_cost,Listed_type,
            Listed_city,Pricing_segment,Rating_category)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"""

cursor.executemany(query,data)
conn.commit()


# 1. Which Bangalore locations have the highest average restaurant ratings?
query1="""
        SELECT Location, ROUND(avg(Rating),2) as Avg_rating
        FROM restaurants
        GROUP BY Location
        ORDER BY Avg_rating DESC
        LIMIT 10 """

df1_q1=pd.read_sql_query(query1,conn)
#print(df1_q1)

#2.Which locations are over-saturated with restaurants?
query2="""
        SELECT Location, count(*) as Restaurant_count
        FROM restaurants
        GROUP BY Location
        ORDER BY Restaurant_count DESC
        LIMIT 10"""
df1_q2=pd.read_sql_query(query2,conn)
#print(df1_q2)

#3. Does online ordering improve restaurant ratings?
query3="""
        SELECT Online_order,ROUND(avg(Rating),2) as Avg_rating
        FROM restaurants
        GROUP BY Online_order"""
df1_q3=pd.read_sql_query(query3,conn)
#print(df1_q3)

#4.What price range delivers the best customer satisfaction?
query4="""
        SELECT Pricing_segment,ROUND(avg(Rating),2) as Avg_rating
        FROM restaurants
        GROUP BY Pricing_segment
        ORDER BY Avg_rating DESC"""
df1_q4=pd.read_sql_query(query4,conn)
#print(df1_q4)

#5.Which cuisines are most common in Bangalore?
query5="""
        SELECT Cusines,count(*) as Cusine_count
        FROM restaurants
        GROUP BY Cusines
        ORDER BY Cusine_count DESC
        Limit 10"""
df1_q5=pd.read_sql_query(query5,conn)
#print(df1_q5)

#6.Which cuisines receive the highest average ratings?
query6="""
        SELECT Cusines,ROUND(avg(Rating),2) as Avg_rating
        FROM restaurants
        GROUP BY Cusines
        ORDER BY Avg_rating DESC
        Limit 10"""
df1_q6=pd.read_sql_query(query6,conn)
#print(df1_q6)

#7.Which cuisines perform well despite having fewer restaurants?
query7="""
        SELECT Cusines,count(*) as Restaurant_count, ROUND(avg(Rating),2) as Avg_rating
        FROM restaurants
        GROUP BY Cusines
        HAVING Restaurant_count < 25
        ORDER BY Avg_rating DESC
        Limit 20"""
df1_q7=pd.read_sql_query(query7,conn)
#print(df1_q7)

#8.What is the relationship between restaurant cost and rating?
query8="""
        SELECT Approx_cost,ROUND(avg(Rating),2) as Avg_rating
        FROM restaurants
        GROUP BY Approx_cost
        ORDER BY Avg_rating DESC
        Limit 10"""
df1_q8=pd.read_sql_query(query8,conn)
#print(df1_q8)

#9.Which locations show high demand but lower average ratings?
query9="""
        SELECT Location,ROUND(avg(Rating),2) as Avg_rating,count(*) as Restaurant_count
        FROM restaurants
        GROUP BY Location
        HAVING Avg_rating<3.5 and Restaurant_count<1000
        ORDER BY Avg_rating """
        
df1_q9=pd.read_sql_query(query9,conn)
#print(df1_q9)

#10.Do restaurants offering both online ordering and table booking perform better?
query10="""
        SELECT Online_order,Book_table,ROUND(avg(Rating),2) as Avg_rating
        FROM restaurants
        WHERE Online_order='Yes' and Book_table='Yes'
        GROUP BY Online_order,Book_table
        ORDER BY Avg_rating  DESC"""
        
df1_q10=pd.read_sql_query(query10,conn)
#print(df1_q10)

#11.Which restaurants are top performers within each pricing segment?
query11="""
        SELECT *
        FROM(
        SELECT Name,Pricing_segment,Rating,
        ROW_NUMBER() over(PARTITION BY Pricing_segment ORDER BY  Rating DESC) as Price_rank
        FROM restaurants) as Ranked
        WHERE Price_rank=1
        ORDER BY RATING DESC
        """
df1_q11=pd.read_sql_query(query11,conn)
#print(df1_q11)

#Order json to df 
#print(df2.info())

cursor.execute("""
        CREATE TABLE IF NOT EXISTS orders(
                order_id VARCHAR(100),
               restaurant_name VARCHAR(50),
               order_date DATE,
               order_value DECIMAL(10,2),
               discount_used VARCHAR(10),
               payment_method VARCHAR(20)
               )""")
data_order=df2.values.tolist()

query_order="""
        INSERT INTO orders(order_id,restaurant_name,
        order_date,order_value,discount_used,payment_method)
        VALUES(?,?,?,?,?,?)"""

cursor.executemany(query_order,data_order)
conn.commit()


#1.Find date wise revenue?
query_order1="""
        SELECT order_date,sum(order_value) as revenue
        FROM orders
        GROUP BY order_date
        ORDER BY order_date DESC
        Limit 20"""
df2_q1=pd.read_sql_query(query_order1,conn)
#print(df2_q1)

#2.What is the relationship of each payment method and order value?
query_order2="""
        SELECT payment_method,ROUND(avg(order_value),3) as avg_value
        FROM orders
        GROUP BY payment_method
        ORDER BY avg_value DESC"""
df2_q2=pd.read_sql_query(query_order2,conn)
#print(df2_q2)

#3.Which restaurants have more orders on specific date?
query_order3="""
        SELECT restaurant_name,order_date,count(*) as order_count
        FROM orders
        GROUP BY restaurant_name,order_date
        ORDER BY order_count DESC
        Limit 10"""
df2_q3=pd.read_sql_query(query_order3,conn)
#print(df2_q3)

#4. Is there correlation between discount_used and order_value?
query_order4="""
        SELECT discount_used,ROUND(avg(order_value),3) as avg_value
        FROM orders
        GROUP BY discount_used
        ORDER BY avg_value DESC"""
df2_q4=pd.read_sql_query(query_order4,conn)
#print(df2_q4)

#5.What are the days when discounts are used the most?
query_order5="""
        SELECT order_date,count(discount_used) as discount_count
        FROM orders
        GROUP BY order_date
        ORDER BY discount_count DESC
        Limit 10"""
df2_q5=pd.read_sql_query(query_order5,conn)
#print(df2_q5)

df_res=pd.read_sql('SELECT * FROM restaurants',conn)

df_order=pd.read_sql("SELECT * FROM orders",conn)

st.set_page_config(
        page_title="RESTAURANT INTELLIGENT SYSTEM",
        layout="wide",
        initial_sidebar_state="expanded"
)

page=st.sidebar.radio("ANALYSIS",["Main Page","Dashboard","Restaurant Q/A","Orders Q/A"])
 
if page=="Main Page":
    st.title("UBER EATS BANGALORE RESTAURANT INTELLIGENCE AND DECISION SUPPORT SYSTEM")
    st.divider()
    st.markdown("""
        - Dashboard page provides multiple filtering options for users
        - Restaurant Q/A displays analysis for predefined business questions
        - Orders Q/A provides all order-related analysis using SQL queries
    """)
elif page=="Dashboard":
    st.title("Uber_eats Dashboard")
    col1,col2,col3,col4,col5=st.columns(5)
    with col1:
        Listed_city= st.selectbox("Select Location", options=df_res['Listed_city'].unique())
    with col2:
        Online_order=st.selectbox("Select order type", options=df_res['Online_order'].unique())
    with col3:
        Rating_category=st.selectbox("Select rating_category", options=df_res['Rating_category'].unique())
    with col4:
        Listed_type=st.selectbox("Select type of restaurant", options=df_res['Listed_type'].unique())
    with col5:
        pricing_segment=st.selectbox("Select pricing segment", options=df_res['Pricing_segment'].unique())

    filtered_df=df_res[(df_res['Listed_city']==Listed_city) & (df_res['Online_order']==Online_order) & (df_res['Rating_category']==Rating_category) & (df_res['Listed_type']==Listed_type) & (df_res['Pricing_segment']==pricing_segment)]
    row_limit = st.slider("Number of rows to display", min_value=5, max_value=50, value=20, step=5)

    st.dataframe(filtered_df.head(row_limit),width='stretch')

elif page=="Restaurant Q/A":
    st.title("Restaurant Q/A")
    question=st.selectbox("Select a Question",[
        "select",
        "Q1. Which Bangalore locations have the highest average restaurant ratings?",
        "Q2. Which locations are over-saturated with restaurants?",
        "Q3. Does online ordering improve restaurant ratings?",
        "Q4. What price range delivers the best customer satisfaction?",
        "Q5. Which cuisines are most common in Bangalore?",
        "Q6. Which cuisines receive the highest average ratings",
        "Q7. Which cuisines perform well despite having fewer restaurants?",
        "Q8. What is the relationship between restaurant cost and rating?",
        "Q9. Which locations show high demand but lower average ratings?",
        "Q10. Do restaurants offering both online ordering and table booking perform better?",
        "Q11. Which restaurants are top performers within each pricing segment?"
         ])

    if "Q1. Which Bangalore locations have the highest average restaurant ratings?" in question:
        st.dataframe(df1_q1,width="content")
    elif "Q2. Which locations are over-saturated with restaurants?" in question:
        st.dataframe(df1_q2,width="content")
    elif "Q3. Does online ordering improve restaurant ratings?" in question:
        st.dataframe(df1_q3,width="content")
    elif "Q4. What price range delivers the best customer satisfaction?" in question:
        st.dataframe(df1_q4,width="content")
    elif "Q5. Which cuisines are most common in Bangalore?" in question:
        st.dataframe(df1_q5,width="content")
    elif "Q6. Which cuisines receive the highest average ratings" in question:
        st.dataframe(df1_q6,width="content")
    elif "Q7. Which cuisines perform well despite having fewer restaurants?" in question:
        st.dataframe(df1_q7,width="content")
    elif "Q8. What is the relationship between restaurant cost and rating?" in question:
        st.dataframe(df1_q8,width="content")
    elif "Q9. Which locations show high demand but lower average ratings?" in question:
        st.dataframe(df1_q9,width="content")
    elif "Q10. Do restaurants offering both online ordering and table booking perform better?" in question:
        st.dataframe(df1_q10,width="content")
    elif "Q11. Which restaurants are top performers within each pricing segment?" in question:
        st.dataframe(df1_q11,width="content")

elif page=="Orders Q/A":
    st.title("Orders Q/A")
    question=st.selectbox("Select a Question",[
        "Select",
        "Q1. Find date wise revenue?",
        "Q2. What is the relationship of each payment method and order value?",
        "Q3. Which restaurants have more orders on specific date?",
        "Q4. Is there correlation between discount_used and order_value?",
        "Q5. What are the days when discounts are used the most?"
    ])
    if "Q1. Find date wise revenue?" in question:
        st.dataframe(df2_q1,width="content")
    elif "Q2. What is the relationship of each payment method and order value?" in question:
        st.dataframe(df2_q2,width="content")
    elif "Q3. Which restaurants have more orders on specific date?" in question:
        st.dataframe(df2_q3,width="content")
    elif "Q4. Is there correlation between discount_used and order_value?" in question:
        st.dataframe(df2_q4,width="content")
    elif  "Q5. What are the days when discounts are used the most?" in question:
        st.dataframe(df2_q5,width="content")


cursor.close()
conn.close()

