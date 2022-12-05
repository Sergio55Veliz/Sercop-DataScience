text="""
dfOpen2019["year"]="2019"
dfOpen2020["year"]="2020"
dfOpen2021["year"]="2021"
dfOpen2022["year"]="2022"
dfOpen=pd.concat([dfOpen2019,dfOpen2020,dfOpen2021,dfOpen2022])
"""
print(text.replace("Open","Selective"))