import streamlit as st
import csv
import gzip
from datetime import datetime
import pandas as pd
import numpy as np
import boto3
from st_files_connection import FilesConnection
import io

def read_idrive():
    idrive = boto3.client( "s3", 
                     aws_access_key_id = st.secrets["key"], 
                     aws_secret_access_key = st.secrets["secret"],
                     endpoint_url = st.secrets["endpoint"],
                     )
    data = idrive.get_object( Bucket="pivox", Key="freeman/telemetry/freeman-master.csv.gz" )
    contents = gzip.decompress( data['Body'].read() )
    return contents

def screen_data( variable, value ):
    # Add acceptable min/max for screening data based on var?
    try:
        if int( value ) != -9999: return True
    except: pass
    try: 
        if value == "0b0" or value == "0b1": return True
    except: pass
    try:
        if value == "0b00" or value == "0b01" \
            or value == "0b10" or value == "0b11":
            return True
    except: pass
    return False

def plot_chart( data, plot_var_1="", plot_var_2="" , open_date="", close_date=""):
    if open_date is None or open_date == "": open_date = datetime( 2020, 3, 9, 0, 0, 0)
    else: open_date = datetime( open_date.year, open_date.month, open_date.day, 0, 0, 0 )
    if close_date is None or close_date == "": close_date = datetime.now
    else: close_date = datetime( close_date.year, close_date.month, close_date.day, 0, 0, 0 )

    if plot_var_1 != "" or plot_var_2 != "":

        csvfile = io.StringIO( data )
        reader = csv.DictReader( csvfile, delimiter="," )
        var_names = reader.fieldnames
        x = []
        row_pos = 0
            
        if plot_var_1 != "" and plot_var_2 != "":
            y1=[]
            y2=[]
            for row in reader:
                if row_pos == 0:
                    x_units = row[ var_names[0] ] +" "+ row[ var_names[1] ]
                    y1_units = row[ plot_var_1 ]
                    y2_units = row[ plot_var_2 ]
                else:
                    timestamp = datetime.strptime( row[ var_names[0] ] +" "+ row[ var_names[1] ], "%Y/%m/%d %H:%M:%S" )
                    if timestamp >= open_date and timestamp <= close_date:
                        try: y1_val = float( row[ plot_var_1 ] )
                        except: y1_val = row[ plot_var_1 ]
                        try: y2_val = float( row[ plot_var_2 ] )
                        except: y2_val = row[ plot_var_2 ]
                        if screen_data( row[ plot_var_1 ], y1_val ) \
                            and screen_data( row[ plot_var_2 ], y2_val ):
                            x.append( timestamp )
                            y1.append( y1_val )
                            y2.append( y2_val )

                row_pos += 1
            data = { "Timestamp": x, plot_var_1: y1, plot_var_2: y2 }
            data_frame = pd.DataFrame( data )
            st.line_chart( data_frame, x="Timestamp", y=[plot_var_1, plot_var_2], height=500 )
            
        elif plot_var_1 != "":
            y1=[]
            for row in reader:
                if row_pos == 0:
                    x_units = row[ var_names[0] ] +" "+ row[ var_names[1] ]
                    y1_units = row[ plot_var_1 ]
                else:
                    timestamp = datetime.strptime( row[ var_names[0] ] +" "+ row[ var_names[1] ], "%Y/%m/%d %H:%M:%S" )
                    if timestamp >= open_date and timestamp <= close_date:
                        try: y1_val = float( row[ plot_var_1 ] )
                        except: y1_val = row[ plot_var_1 ]
                        if screen_data( row[ plot_var_1 ], y1_val ):
                            x.append( timestamp )
                            y1.append( y1_val )
                row_pos += 1
            data = { "Timestamp": x, plot_var_1: y1 }
            data_frame = pd.DataFrame( data )
            st.line_chart( data_frame, x="Timestamp", y=plot_var_1, height=500 )
            
        else:
            y2=[]
            for row in reader:
                if row_pos == 0:
                    x_units = row[ var_names[0] ] +" "+ row[ var_names[1] ]
                    y2_units = row[ plot_var_2 ]
                else:
                    timestamp = datetime.strptime( row[ var_names[0] ] +" "+ row[ var_names[1] ], "%Y/%m/%d %H:%M:%S" )
                    if timestamp >= open_date and timestamp <= close_date:
                        try: y2_val = float( row[ plot_var_1 ] )
                        except: y2_val = row[ plot_var_2 ]
                        if screen_data( row[ plot_var_2 ], y2_val ):
                            x.append( timestamp )
                            y2.append( y2_val )
                row_pos += 1
            data = { "Timestamp": x, plot_var_2: y2 }
            data_frame = pd.DataFrame( data )
            st.line_chart( data_frame, x="Timestamp", y=plot_var_2, height=500 )

st.set_page_config( page_title="Freeman", page_icon="👋" )
st.write("# Freeman Pivox, Idaho City, ID")
left_link, mid_link, right_link = st.columns(3)
left_link.page_link( "dashboard.py", label="Dashboard" )
mid_link.page_link( "pages/images.py", label="Images" )

data = read_idrive().decode( 'iso8859_2' )
csvfile = io.StringIO( data )
reader = csv.DictReader( csvfile, delimiter="," )
var_names = reader.fieldnames

top_left, top_right = st.columns( 2 )
plot_var_1 = top_left.selectbox( "Select Variable 1", [""] + var_names )
plot_var_2 = top_right.selectbox( "Select Variable 2", [""] + var_names )

bot_left, bot_middle, bot_right = st.columns( 3, vertical_alignment = "bottom" )
open_date = bot_left.date_input( "Begin Date", value = None )
close_date = bot_middle.date_input( "End Date", value = "today" )
if bot_right.button( "Plot Chart", use_container_width=True):
    plot_chart( data, plot_var_1, plot_var_2, open_date, close_date )

#st.write( "You selected:", var_names )